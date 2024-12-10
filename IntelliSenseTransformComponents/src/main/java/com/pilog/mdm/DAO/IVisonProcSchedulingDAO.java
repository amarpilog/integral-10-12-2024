/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;

import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.PilogUtilities;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import oracle.sql.CLOB;
import oracle.sql.RAW;
import org.json.simple.JSONObject;
import org.quartz.CronExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author Ravindar.P
 */
@Repository
public class IVisonProcSchedulingDAO {

    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;
    
     PilogUtilities visionUtills = new PilogUtilities();

    @Autowired
    private DataAccess access;

    public JSONObject saveProcShedulingInfo(HttpServletRequest request, JSONObject paramData) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");

            String orgnId = (String) paramData.get("orgnId");
            String sessionUserName = (String) paramData.get("userName");
            String procedureName = (String) paramData.get("procedureName");
            String connectionName = (String) paramData.get("connectionName");
            String connectionUrl = (String) paramData.get("connectionUrl");
            String connectionObject = (String) paramData.get("connectionObject");
            String cronExp = (String) paramData.get("cronExp");
            String cronStartDate = (String) paramData.get("cronStartDate");
            String cronEndDate = (String) paramData.get("cronEndDate");

            java.util.Date nextValidTime = new CronExpression(cronExp).getNextValidTimeAfter(new java.util.Date());
            java.sql.Date nextValidTimeSqlDate = new java.sql.Date(nextValidTime.getTime());
            Class.forName(dataBaseDriver);
            connection = DriverManager.getConnection(dbURL, userName, password);
            String insertQuery = "INSERT INTO C_ETL_PROC_SCHEDULING("
                    + "ORGN_ID, " //1
                    + "USER_NAME, "//2 
                    + "PROCEDURE_NAME, "//3
                    + "CONNECTION_NAME, "//4
                    + "CONNECTION_URL, "//5
                    + "CONNECTION_OBJECT, "//6
                    + "CRON_TRIGGER, "//7
                    + "START_DATE, "//8
                    + "END_DATE, "//9
                    + "NXT_RUNNING_DATE, "//10
                    + "LAST_RUNNING_DATE, "//11
                    + "ACTIVE_FLAG, "//12
                    + "RUNNING_STATUS,  "//13
                    + "CREATE_BY, "//14
                    + "EDIT_BY )"//15
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            System.out.println("insertQuery::" + insertQuery);
            preparedStatement = connection.prepareStatement(insertQuery);
            Map<Integer, Object> insertMap = new HashMap<>();
            preparedStatement.setObject(1, orgnId);//ORGN_ID
            preparedStatement.setObject(2, sessionUserName);//USER_NAME
            preparedStatement.setObject(3, procedureName);//PROCEDURE_NAME
            preparedStatement.setObject(4, connectionName);//CONNECTION_NAME
            preparedStatement.setObject(5, connectionUrl);//CONNECTION_URL
            preparedStatement.setObject(6, connectionObject);//CONNECTION_OBJECT
            preparedStatement.setObject(7, cronExp);//CRON_TRIGGER
            preparedStatement.setObject(8, cronStartDate != null && !"".equalsIgnoreCase(cronStartDate)
                    ? new java.sql.Date(dateFormat.parse(cronStartDate).getTime()) : null);//START_DATE
            preparedStatement.setObject(9, cronEndDate);//END_DATE
            preparedStatement.setObject(10, nextValidTimeSqlDate);//NXT_RUNNING_DATE
            preparedStatement.setObject(11, null);//LAST_RUNNING_DATE
            preparedStatement.setObject(12, "Y");//ACTIVE_FLAG
            preparedStatement.setObject(13, "RUNNING");//RUNNING_STATUS
            preparedStatement.setObject(14, sessionUserName);//CREATE_BY
            preparedStatement.setObject(15, sessionUserName);//EDIT_BY

            int insertCount = preparedStatement.executeUpdate();
            if (insertCount != 0) {
                resultObj.put("flag", true);
                resultObj.put("message", "Selected Proc is running.");
            } else {
                resultObj.put("flag", false);
                resultObj.put("message", "Unable to run the selected Proc.");
            }
        } catch (SQLIntegrityConstraintViolationException ex) {
            ex.printStackTrace();
            resultObj.put("flag", false);
            resultObj.put("message", "The Selected Proc is in-process,please go to the Schedule Proc tabs and check the progress.");
        } catch (Exception e) {
            resultObj.put("flag", false);
            resultObj.put("message", e.getMessage());
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
        return resultObj;
    }

    public JSONObject getScheduledProcs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        List<Object[]> procsList = new ArrayList();
        try {
            String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
            String ssOrgId = (String) request.getSession(false).getAttribute("ssOrgId");
            String selectQuery = "SELECT "
                    + "ORGN_ID, " //0
                    + "USER_NAME, "//1 
                    + "PROCEDURE_NAME, "//2
                    + "CONNECTION_NAME, "//3
                    + "CONNECTION_URL, "//4
                    + "CONNECTION_OBJECT, "//5
                    + "CRON_TRIGGER, "//6
                    + "START_DATE, "//7
                    + "END_DATE, "//8
                    + "NXT_RUNNING_DATE, "//9
                    + "LAST_RUNNING_DATE, "//10
                    + "ACTIVE_FLAG, "//11
                    + "RUNNING_STATUS,  "//12
                    + "CREATE_BY, "//13
                    + "EDIT_BY "//14
                    + "FROM C_ETL_PROC_SCHEDULING"
                    + " WHERE ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME";
            Map selectMap = new HashMap();
            selectMap.put("ORGN_ID", ssOrgId);
            selectMap.put("USER_NAME", ssUsername);
            System.out.println("selectQuery::" + selectQuery);
            procsList = access.sqlqueryWithParams(selectQuery, selectMap);

        } catch (Exception e) {

            e.printStackTrace();
        } finally {

        }
        resultObj.put("procsList", procsList);
        return resultObj;
    }

    public int updateProcActiveStatus(String procedureName, String url, String ssOrgId, String ssUsername, String activeFlag, String runningStatus) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE C_ETL_PROC_SCHEDULING SET ACTIVE_FLAG=:ACTIVE_FLAG,"
                    + "RUNNING_STATUS=:RUNNING_STATUS "
                    + "WHERE ORGN_ID=:ORGN_ID "
                    + " AND USER_NAME=:USER_NAME "
                    + " AND PROCEDURE_NAME=:PROCEDURE_NAME  "
                    + " AND CONNECTION_URL=:CONNECTION_URL ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", ssOrgId);
            updateMap.put("USER_NAME", ssUsername);
            updateMap.put("PROCEDURE_NAME", procedureName);
            updateMap.put("CONNECTION_URL", url);
            updateMap.put("ACTIVE_FLAG", activeFlag);
            updateMap.put("RUNNING_STATUS", runningStatus);
            System.out.println("updateQuery:::" + updateQuery);
            System.out.println("updateMap:::" + updateMap);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }

    public int deleteScheduledProc(String procedureName, String url, String ssOrgId, String ssUsername) {
        int deleteCount = 0;
        try {
            String deleteQuery = "DELETE FROM C_ETL_PROC_SCHEDULING WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND "
                    + " PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> deleteMap = new HashMap<>();
            deleteMap.put("ORGN_ID", ssOrgId);
            deleteMap.put("USER_NAME", ssUsername);
            deleteMap.put("PROCEDURE_NAME", procedureName);
            deleteMap.put("CONNECTION_URL", url);
            System.out.println("updateQuery:::" + deleteQuery);
            System.out.println("updateMap:::" + deleteMap);
            deleteCount = access.executeUpdateSQLNoAudit(deleteQuery, deleteMap);
            System.out.println("updateCount:::" + deleteCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return deleteCount;
    }

    public List getActiveScheduledProcs() {
        List<Object[]> procsList = new ArrayList();
        try {
            String selectQuery = "SELECT "
                    + "ORGN_ID, " //0
                    + "USER_NAME, "//1 
                    + "PROCEDURE_NAME, "//2
                    + "CONNECTION_NAME, "//3
                    + "CONNECTION_URL, "//4
                    + "CONNECTION_OBJECT, "//5
                    + "CRON_TRIGGER, "//6
                    + "START_DATE, "//7
                    + "END_DATE, "//8
                    + "NXT_RUNNING_DATE, "//9
                    + "LAST_RUNNING_DATE, "//10
                    + "ACTIVE_FLAG, "//11
                    + "RUNNING_STATUS,  "//12
                    + "CREATE_BY, "//13
                    + "EDIT_BY "//14
                    + "FROM C_ETL_PROC_SCHEDULING"
                    + " WHERE C_ETL_PROC_SCHEDULING.ACTIVE_FLAG =:ACTIVE_FLAG AND "
                    + " C_ETL_PROC_SCHEDULING.RUNNING_STATUS =:RUNNING_STATUS ORDER BY ORGN_ID";
            Map selectMap = new HashMap();
            selectMap.put("ACTIVE_FLAG", "Y");
            selectMap.put("RUNNING_STATUS", "RUNNING");
            System.out.println("selectQuery::" + selectQuery);
            procsList = access.sqlqueryWithParams(selectQuery, selectMap);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return procsList;
    }

    public int fetchRemoveProcFromScheduler(String procedureName, String url, String ssOrgId, String ssUsername) {
        int fetchCount = 0;
        try {
            String fetchQuery = "SELECT "
                    + "ORGN_ID, " //0
                    + "USER_NAME, "//1 
                    + "PROCEDURE_NAME, "//2
                    + "CONNECTION_NAME, "//3
                    + "CONNECTION_URL, "//4
                    + "CONNECTION_OBJECT, "//5
                    + "CRON_TRIGGER, "//6
                    + "START_DATE, "//7
                    + "END_DATE, "//8
                    + "NXT_RUNNING_DATE, "//9
                    + "LAST_RUNNING_DATE, "//10
                    + "ACTIVE_FLAG, "//11
                    + "RUNNING_STATUS,  "//12
                    + "CREATE_BY, "//13
                    + "EDIT_BY "//14
                    + "FROM C_ETL_PROC_SCHEDULING"
                    + " WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND "
                    + " PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL AND "
                    + " RUNNING_STATUS =:RUNNING_STATUS ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", ssOrgId);
            selectMap.put("USER_NAME", ssUsername);
            selectMap.put("PROCEDURE_NAME", procedureName);
            selectMap.put("CONNECTION_URL", url);
            selectMap.put("RUNNING_STATUS", "RUNNING");
            System.out.println("fetchQuery:::" + fetchQuery);
            System.out.println("updateMap:::" + selectMap);
            fetchCount = access.executeUpdateSQLNoAudit(fetchQuery, selectMap);
            System.out.println("updateCount:::" + fetchCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fetchCount;
    }

    public Map fetchProcScheduleInfo(String procedureName, String url, String ssOrgId, String ssUsername) {

        Map procInfoObj = new LinkedHashMap<>();
        try {
            String selectQuery = "SELECT "
                    + "PROCEDURE_NAME, "
                    + "RUNNING_STATUS, "
                    + "START_DATE, "
                    + "END_DATE, "
                    + "LAST_RUNNING_DATE, "
                    + "NXT_RUNNING_DATE "
                    + "FROM C_ETL_PROC_SCHEDULING"
                    + " WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND "
                    + " PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", ssOrgId);
            selectMap.put("USER_NAME", ssUsername);
            selectMap.put("PROCEDURE_NAME", procedureName);
            selectMap.put("CONNECTION_URL", url);

            List<Object[]> procInfoList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (procInfoList != null && !procInfoList.isEmpty()) {
                Object[] rowData = procInfoList.get(0);
                procInfoObj.put("Proc Name", rowData[0] != null ? rowData[0] : "");
                procInfoObj.put("Status", rowData[1] != null ? rowData[1] : "");
                procInfoObj.put("Start Date", rowData[2] != null ? String.valueOf(rowData[2]) : "");
                procInfoObj.put("End Date", rowData[3] != null ? String.valueOf(rowData[3]) : "");
                procInfoObj.put("Last Run Date", rowData[4] != null ? String.valueOf(rowData[4]) : "");
                procInfoObj.put("Next Run Date", rowData[5] != null ? String.valueOf(rowData[5]) : "");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return procInfoObj;
    }

    public Object[] getProcSchedulars(String procedureName, String url, String ssOrgId, String ssUsername) {
        Object[] procSchedularsArray = null;
        List<Object[]> procSchedularsList = new ArrayList<>();
        try {
            String fetchQuery = "SELECT "
                    + "ORGN_ID, " //0
                    + "USER_NAME, "//1 
                    + "PROCEDURE_NAME, "//2
                    + "CONNECTION_NAME, "//3
                    + "CONNECTION_URL, "//4
                    + "CONNECTION_OBJECT, "//5
                    + "CRON_TRIGGER, "//6
                    + "START_DATE, "//7
                    + "END_DATE, "//8
                    + "NXT_RUNNING_DATE, "//9
                    + "LAST_RUNNING_DATE, "//10
                    + "ACTIVE_FLAG, "//11
                    + "RUNNING_STATUS,  "//12
                    + "CREATE_BY, "//13
                    + "EDIT_BY, "//14
                    + "CREATE_DATE, "//15
                    + "EDIT_DATE, "//16
                    + "AUDIT_ID "//17
                    + "FROM C_ETL_PROC_SCHEDULING"
                    + " WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND "
                    + " PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL AND "
                    + " ACTIVE_FLAG =:ACTIVE_FLAG AND "
                    + " RUNNING_STATUS =:RUNNING_STATUS ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", ssOrgId);
            selectMap.put("USER_NAME", ssUsername);
            selectMap.put("PROCEDURE_NAME", procedureName);
            selectMap.put("CONNECTION_URL", url);
            selectMap.put("ACTIVE_FLAG", "Y");
            selectMap.put("RUNNING_STATUS", "RUNNING");
            System.out.println("selectQuery:::" + fetchQuery);
            System.out.println("selectMap:::" + selectMap);
            procSchedularsList = access.sqlqueryWithParams(fetchQuery, selectMap);
            if (procSchedularsList != null && !procSchedularsList.isEmpty()) {
                procSchedularsList = procSchedularsList.stream()
                        .map(objArray -> {
                            try {
                                if (objArray[0] != null
                                        && objArray[0] instanceof byte[]) {
                                    objArray[0] = new RAW((byte[]) objArray[0]).stringValue();
                                }
                            } catch (Exception e) {
                            }
                            return objArray;
                        }).collect(Collectors.toList());

                procSchedularsArray = procSchedularsList.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
        return procSchedularsArray;
    }

    public boolean checkProcRunningStatus(Object[] procSchedularArray) {
        boolean isStop = false;

        try {
            String selectQuery = "SELECT RUNNING_STATUS FROM C_ETL_PROC_SCHEDULING"
                    + " WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND "
                    + " PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", procSchedularArray[0]);
            selectMap.put("USER_NAME", procSchedularArray[1]);
            selectMap.put("PROCEDURE_NAME", procSchedularArray[2]);
            selectMap.put("CONNECTION_URL", procSchedularArray[4]);
            System.out.println("selectQuery:::" + selectQuery);
            System.out.println("selectMap:::" + selectMap);
            List<String> runningStatusList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (runningStatusList != null
                    && !runningStatusList.isEmpty()
                    && "RUNNING".equalsIgnoreCase(String.valueOf(runningStatusList.get(0)))) {
                isStop = true;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isStop;
    }

    public int updateProcRunningStatus(Object[] procSchedularArray, String statusValue) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE C_ETL_PROC_SCHEDULING SET RUNNING_STATUS =:RUNNING_STATUS WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND"
                    + " PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", procSchedularArray[0]);
            updateMap.put("USER_NAME", procSchedularArray[1]);
            updateMap.put("PROCEDURE_NAME", procSchedularArray[2]);
            updateMap.put("CONNECTION_URL", procSchedularArray[4]);
            updateMap.put("RUNNING_STATUS", statusValue);
            System.out.println("updateQuery:::" + updateQuery);
            System.out.println("updateMap:::" + updateMap);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }

    public int updateProcStartDate(String procedureName, String url, Date procStartDate) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE C_ETL_PROC_SCHEDULING SET PROC_START_DATE =:PROC_START_DATE"
                    + " WHERE PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("PROCEDURE_NAME", procedureName);
            updateMap.put("CONNECTION_URL", url);
            updateMap.put("PROC_START_DATE", procStartDate);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }

    public int updateProcNextRunningDate(String procedureName, String url, Date nextRunningDate) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE C_ETL_PROC_SCHEDULING SET NXT_RUNNING_DATE =:NXT_RUNNING_DATE"
                    + " WHERE PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("PROCEDURE_NAME", procedureName);
            updateMap.put("CONNECTION_URL", url);
            updateMap.put("NXT_RUNNING_DATE", nextRunningDate);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }

    public int procDbmsOutput(Object[] procSchedularArray, String dbmsOutput, String outputDate) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE C_ETL_PROC_SCHEDULING SET DBMS_OUTPUT =:DBMS_OUTPUT,OUTPUT_TIMESTAMP =:OUTPUT_TIMESTAMP  "
                    + "WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND"
                    + " PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", procSchedularArray[0]);
            updateMap.put("USER_NAME", procSchedularArray[1]);
            updateMap.put("PROCEDURE_NAME", procSchedularArray[2]);
            updateMap.put("CONNECTION_URL", procSchedularArray[4]);
            updateMap.put("DBMS_OUTPUT", dbmsOutput);
            updateMap.put("OUTPUT_TIMESTAMP", outputDate);
            System.out.println("updateQuery:::" + updateQuery);
            System.out.println("updateMap:::" + updateMap);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }

    public Map fetchProcScheduleLog(String procedureName, String url, String ssOrgId, String ssUsername) {

        Map procLogObj = new LinkedHashMap<>();
        try {
            String selectQuery = "SELECT "
                    + "DBMS_OUTPUT,"
                    + " OUTPUT_TIMESTAMP "
                    + "FROM C_ETL_PROC_SCHEDULING"
                    + " WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND "
                    + " PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", ssOrgId);
            selectMap.put("USER_NAME", ssUsername);
            selectMap.put("PROCEDURE_NAME", procedureName);
            selectMap.put("CONNECTION_URL", url);

           List<Object> procInfoList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (procInfoList != null && !procInfoList.isEmpty()) {
                Object[] rowdata =  (Object[]) procInfoList.get(0);
                String message =visionUtills.clobToString((Clob) rowdata[0]);
                String TimeStamp =  String.valueOf(rowdata[1]);
                procLogObj.put(TimeStamp, message);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return procLogObj;
    }
    
    public int updateProcLastRunningDate(String procedureName, String url, Date lastRunningDate) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE C_ETL_PROC_SCHEDULING SET LAST_RUNNING_DATE =:LAST_RUNNING_DATE"
                    + " WHERE PROCEDURE_NAME =:PROCEDURE_NAME AND "
                    + " CONNECTION_URL =:CONNECTION_URL ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("PROCEDURE_NAME", procedureName);
            updateMap.put("CONNECTION_URL", url);
            updateMap.put("LAST_RUNNING_DATE", lastRunningDate);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }

}
