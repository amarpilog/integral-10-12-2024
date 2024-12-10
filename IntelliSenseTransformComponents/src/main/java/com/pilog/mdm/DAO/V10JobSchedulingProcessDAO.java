/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;


import com.pilog.mdm.access.DataAccess;
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
import oracle.sql.RAW;

import org.json.simple.JSONObject;
import org.quartz.CronExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author sanya
 */
@Repository
public class V10JobSchedulingProcessDAO {

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
    
    @Transactional
    public List<Object[]> getJobsListByJobId(Object jobId, Object orgnId) {
        List<Object[]> jobsList = new ArrayList<>();
        try {
            String selectQuery = "SELECT   ORGN_ID,"//0
                    + "         SEQUENCE_NO,"//1
                    + "         JOB_ID,"//2
                    + "         JOB_DESCR,"//3
                    + "         USER_NAME,"//4
                    + "         TRFM_RULES_ID,"//5
                    + "         TRFM_DEGN_DATA,"//6
                    + "         JOBS_CUST_COL1,"//7
                    + "         JOBS_CUST_COL2,"//8
                    + "         JOBS_CUST_COL3,"//9
                    + "         JOBS_CUST_COL4,"//10
                    + "         JOBS_CUST_COL5,"//11
                    + "         JOBS_CUST_COL6,"//12
                    + "         JOBS_CUST_COL7,"//13
                    + "         JOBS_CUST_COL8,"//14
                    + "         JOBS_CUST_COL9,"//15
                    + "         JOBS_CUST_COL10,"//16
                    + "         AUDIT_ID,"//17
                    + "         CREATE_BY,"//18
                    + "         EDIT_BY,"//19
                    + "         CREATE_DATE,"//20
                    + "         EDIT_DATE"//21
                    + "  FROM   C_DM_JOBS"
                    + " WHERE JOB_ID =:JOB_ID AND ORGN_ID =:ORGN_ID"
                    + " ORDER BY SEQUENCE_NO ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("JOB_ID", jobId);
            selectMap.put("ORGN_ID", orgnId);
            System.out.println("selectQuery:::" + selectQuery);
            System.out.println("selectMap:::" + selectMap);
            jobsList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (jobsList != null && !jobsList.isEmpty()) {
                jobsList = jobsList.stream()
                        .map(objArray -> {
                        try {
                            if (objArray[0] != null 
                                    && objArray[0] instanceof byte[]) {
                                objArray[0] = new RAW((byte[])objArray[0]).stringValue();
                            }
                            if (objArray[2] != null 
                                    && objArray[2] instanceof byte[]) {
                                objArray[2] = new RAW((byte[])objArray[2]).stringValue();
                            }
                    } catch (Exception e) {
                    }
                        return objArray;
                        }).collect(Collectors.toList());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobsList;
    }

    @Transactional
    public List getJobSchedulars() {
        List<Object[]> jobSchedularsList = new ArrayList<>();
        try {
            String selectQuery = "SELECT  "
                    + " DAL_DM_JOB_SCHEDULING.ORGN_ID,"//0
                    + "         DAL_DM_JOB_SCHEDULING.USER_NAME,"//1
                    + "         DAL_DM_JOB_SCHEDULING.JOB_ID,"//2
                    + "         DAL_DM_JOB_SCHEDULING.JOB_NAME,"//3
                    + "         DAL_DM_JOB_SCHEDULING.CRON_TRIGGER,"//4
                    + "         DAL_DM_JOB_SCHEDULING.NXT_RUNNING_DATE,"//5
                    + "         DAL_DM_JOB_SCHEDULING.ACTIVE_FLAG,"//6
                    + "         DAL_DM_JOB_SCHEDULING.RUNNING_STATUS,"//7
                    + "         DAL_DM_JOB_SCHEDULING.JOB_INIT_PARAMS,"//8
                    + "         DAL_DM_JOB_SCHEDULING.SCHEDULE_START_DATE,"//9
                    + "         DAL_DM_JOB_SCHEDULING.SCHEDULE_END_DATE,"//10
                    + "         DAL_DM_JOB_SCHEDULING.JOB_START_DATE,"//11
                    + "         DAL_DM_JOB_SCHEDULING.LAST_RUN_DURATION,"//12
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL5,"//13
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL6,"//14
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL7,"//15
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL8,"//16
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL9,"//17
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL10,"//18
                    + "         DAL_DM_JOB_SCHEDULING.CREATE_BY,"//19
                    + "         DAL_DM_JOB_SCHEDULING.EDIT_BY,"//20
                    + "         DAL_DM_JOB_SCHEDULING.CREATE_DATE,"//21
                    + "         DAL_DM_JOB_SCHEDULING.EDIT_DATE,"//22
                    + "         DAL_DM_JOB_SCHEDULING.AUDIT_ID,"//23
                    + "         DAL_DM_JOB_SCHEDULING.START_DATE,"//24
                    + "         DAL_DM_JOB_SCHEDULING.END_DATE"//25
                    //                    + "         C_DM_JOBS.SEQUENCE_NO,"//24
                    //                    + "         C_DM_JOBS.TRFM_RULES_ID,"//25
                    //                    + "         C_DM_JOBS.TRFM_DEGN_DATA,"//26
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL1,"//27
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL2,"//28
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL3,"//29
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL4,"//30
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL5,"//31
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL6,"//32
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL7,"//33
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL8,"//34
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL9,"//35
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL10"//36
                    + "  FROM      DAL_DM_JOB_SCHEDULING"
                    //                    + "         INNER JOIN"
                    //                    + "            C_DM_JOBS"
                    //                    + "         ON C_DM_JOBS.ORGN_ID = DAL_DM_JOB_SCHEDULING.ORGN_ID"
                    //                    + "            AND C_DM_JOBS.JOB_ID = DAL_DM_JOB_SCHEDULING.JOB_ID"
                    + " WHERE DAL_DM_JOB_SCHEDULING.ACTIVE_FLAG =:ACTIVE_FLAG AND "
                    + " DAL_DM_JOB_SCHEDULING.RUNNING_STATUS =:RUNNING_STATUS ORDER BY ORGN_ID";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ACTIVE_FLAG", "Y");
            selectMap.put("RUNNING_STATUS", "STOP");
            System.out.println("selectQuery:::" + selectQuery);
            System.out.println("selectMap:::" + selectMap);
            jobSchedularsList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (jobSchedularsList != null && !jobSchedularsList.isEmpty()) {
                jobSchedularsList = jobSchedularsList.stream()
                        .map(objArray -> {
                            try {
                                if (objArray[0] != null
                                        && objArray[0] instanceof byte[]) {
                                    objArray[0] = new RAW((byte[]) objArray[0]).stringValue();
                                }
                                if (objArray[2] != null
                                        && objArray[2] instanceof byte[]) {
                                    objArray[2] = new RAW((byte[]) objArray[2]).stringValue();
                                }
                            } catch (Exception e) {
                            }
                            return objArray;
                        }).collect(Collectors.toList()); 
            }
            
        } catch (Exception e) {
            e.printStackTrace();

        }
        return jobSchedularsList;
    }
    @Transactional
    public Object[] getJobSchedulars(Object jobId,Object orgnId,String userName) {
        Object[] jobSchedularsArray = null;
        List<Object[]> jobSchedularsList = new ArrayList<>();
        try {
            String selectQuery = "SELECT  "
                    + " DAL_DM_JOB_SCHEDULING.ORGN_ID,"//0
                    + "         DAL_DM_JOB_SCHEDULING.USER_NAME,"//1
                    + "         DAL_DM_JOB_SCHEDULING.JOB_ID,"//2
                    + "         DAL_DM_JOB_SCHEDULING.JOB_NAME,"//3
                    + "         DAL_DM_JOB_SCHEDULING.CRON_TRIGGER,"//4
                    + "         DAL_DM_JOB_SCHEDULING.NXT_RUNNING_DATE,"//5
                    + "         DAL_DM_JOB_SCHEDULING.ACTIVE_FLAG,"//6
                    + "         DAL_DM_JOB_SCHEDULING.RUNNING_STATUS,"//7
                    + "         DAL_DM_JOB_SCHEDULING.JOB_INIT_PARAMS,"//8
                    + "         DAL_DM_JOB_SCHEDULING.SCHEDULE_START_DATE,"//9
                    + "         DAL_DM_JOB_SCHEDULING.SCHEDULE_END_DATE,"//10
                    + "         DAL_DM_JOB_SCHEDULING.JOB_START_DATE,"//11
                    + "         DAL_DM_JOB_SCHEDULING.LAST_RUN_DURATION,"//12
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL5,"//13
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL6,"//14
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL7,"//15
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL8,"//16
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL9,"//17
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL10,"//18
                    + "         DAL_DM_JOB_SCHEDULING.CREATE_BY,"//19
                    + "         DAL_DM_JOB_SCHEDULING.EDIT_BY,"//20
                    + "         DAL_DM_JOB_SCHEDULING.CREATE_DATE,"//21
                    + "         DAL_DM_JOB_SCHEDULING.EDIT_DATE,"//22
                    + "         DAL_DM_JOB_SCHEDULING.AUDIT_ID,"//23
                    + "         DAL_DM_JOB_SCHEDULING.START_DATE,"//24
                    + "         DAL_DM_JOB_SCHEDULING.END_DATE"//25
                    //                    + "         C_DM_JOBS.SEQUENCE_NO,"//24
                    //                    + "         C_DM_JOBS.TRFM_RULES_ID,"//25
                    //                    + "         C_DM_JOBS.TRFM_DEGN_DATA,"//26
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL1,"//27
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL2,"//28
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL3,"//29
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL4,"//30
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL5,"//31
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL6,"//32
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL7,"//33
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL8,"//34
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL9,"//35
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL10"//36
                    + "  FROM      DAL_DM_JOB_SCHEDULING"
                    //                    + "         INNER JOIN"
                    //                    + "            C_DM_JOBS"
                    //                    + "         ON C_DM_JOBS.ORGN_ID = DAL_DM_JOB_SCHEDULING.ORGN_ID"
                    //                    + "            AND C_DM_JOBS.JOB_ID = DAL_DM_JOB_SCHEDULING.JOB_ID"
                    + " WHERE DAL_DM_JOB_SCHEDULING.ACTIVE_FLAG =:ACTIVE_FLAG AND "
                    + " DAL_DM_JOB_SCHEDULING.RUNNING_STATUS =:RUNNING_STATUS "
                    + " AND DAL_DM_JOB_SCHEDULING.ORGN_ID =:ORGN_ID "
                    + " AND DAL_DM_JOB_SCHEDULING.USER_NAME =:USER_NAME "
                    + " AND DAL_DM_JOB_SCHEDULING.JOB_ID =:JOB_ID ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ACTIVE_FLAG", "Y");
            selectMap.put("RUNNING_STATUS", "STOP");
            selectMap.put("ORGN_ID", orgnId);
            selectMap.put("USER_NAME", userName);
            selectMap.put("JOB_ID", jobId);
            System.out.println("selectQuery:::" + selectQuery);
            System.out.println("selectMap:::" + selectMap);
            jobSchedularsList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (jobSchedularsList != null && !jobSchedularsList.isEmpty()) {
                    jobSchedularsList = jobSchedularsList.stream()
                            .map(objArray -> {
                                try {
                                    if (objArray[0] != null
                                            && objArray[0] instanceof byte[]) {
                                        objArray[0] = new RAW((byte[]) objArray[0]).stringValue();
                                    }
                                    if (objArray[2] != null
                                            && objArray[2] instanceof byte[]) {
                                        objArray[2] = new RAW((byte[]) objArray[2]).stringValue();
                                    }
                                } catch (Exception e) {
                                }
                                return objArray;
                            }).collect(Collectors.toList());
                
                jobSchedularsArray = jobSchedularsList.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
        return jobSchedularsArray;
    }

    @Transactional
    public boolean checkRunningStatus(Object[] jobSchedularArray) {
        boolean isStop = false;

        try {
            String selectQuery = "SELECT RUNNING_STATUS FROM DAL_DM_JOB_SCHEDULING WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND JOB_ID =:JOB_ID ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", jobSchedularArray[0]);
            selectMap.put("USER_NAME", jobSchedularArray[1]);
            selectMap.put("JOB_ID", jobSchedularArray[2]);
            System.out.println("selectQuery:::" + selectQuery);
            System.out.println("selectMap:::" + selectMap);
            List<String> runningStatusList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (runningStatusList != null
                    && !runningStatusList.isEmpty()
                    && "STOP".equalsIgnoreCase(String.valueOf(runningStatusList.get(0)))) {
                isStop = true;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isStop;
    }

    @Transactional
    public int updateRunningStatus(Object[] jobSchedularArray, String statusValue) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE DAL_DM_JOB_SCHEDULING SET RUNNING_STATUS =:RUNNING_STATUS WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND JOB_ID =:JOB_ID ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", jobSchedularArray[0]);
            updateMap.put("USER_NAME", jobSchedularArray[1]);
            updateMap.put("JOB_ID", jobSchedularArray[2]);
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
    @Transactional
    public List getActiveScheduledJobs(String orgnId,String sessionUserName) {
        List<Object[]> jobSchedularsList = new ArrayList<>();
        try {
            String selectQuery = "SELECT  "
                    + " DAL_DM_JOB_SCHEDULING.ORGN_ID,"//0
                    + "         DAL_DM_JOB_SCHEDULING.USER_NAME,"//1
                    + "         DAL_DM_JOB_SCHEDULING.JOB_ID,"//2
                    + "         DAL_DM_JOB_SCHEDULING.JOB_NAME,"//3
                    + "         DAL_DM_JOB_SCHEDULING.CRON_TRIGGER,"//4
                    + "         DAL_DM_JOB_SCHEDULING.NXT_RUNNING_DATE,"//5
                    + "         DAL_DM_JOB_SCHEDULING.ACTIVE_FLAG,"//6
                    + "         DAL_DM_JOB_SCHEDULING.RUNNING_STATUS,"//7
                    + "         DAL_DM_JOB_SCHEDULING.JOB_INIT_PARAMS,"//8
                    + "         DAL_DM_JOB_SCHEDULING.SCHEDULE_START_DATE,"//9
                    + "         DAL_DM_JOB_SCHEDULING.SCHEDULE_END_DATE,"//10
                    + "         DAL_DM_JOB_SCHEDULING.JOB_START_DATE,"//11
                    + "         DAL_DM_JOB_SCHEDULING.LAST_RUN_DURATION,"//12
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL5,"//13
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL6,"//14
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL7,"//15
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL8,"//16
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL9,"//17
                    + "         DAL_DM_JOB_SCHEDULING.JOB_CUST_COL10,"//18
                    + "         DAL_DM_JOB_SCHEDULING.CREATE_BY,"//19
                    + "         DAL_DM_JOB_SCHEDULING.EDIT_BY,"//20
                    + "         DAL_DM_JOB_SCHEDULING.CREATE_DATE,"//21
                    + "         DAL_DM_JOB_SCHEDULING.EDIT_DATE,"//22
                    + "         DAL_DM_JOB_SCHEDULING.AUDIT_ID"//23
                    //                    + "         C_DM_JOBS.SEQUENCE_NO,"//24
                    //                    + "         C_DM_JOBS.TRFM_RULES_ID,"//25
                    //                    + "         C_DM_JOBS.TRFM_DEGN_DATA,"//26
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL1,"//27
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL2,"//28
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL3,"//29
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL4,"//30
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL5,"//31
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL6,"//32
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL7,"//33
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL8,"//34
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL9,"//35
                    //                    + "         C_DM_JOBS.JOBS_CUST_COL10"//36
                    + "  FROM      DAL_DM_JOB_SCHEDULING"
                    //                    + "         INNER JOIN"
                    //                    + "            C_DM_JOBS"
                    //                    + "         ON C_DM_JOBS.ORGN_ID = DAL_DM_JOB_SCHEDULING.ORGN_ID"
                    //                    + "            AND C_DM_JOBS.JOB_ID = DAL_DM_JOB_SCHEDULING.JOB_ID"
                    + " WHERE DAL_DM_JOB_SCHEDULING.ORGN_ID =:ORGN_ID AND "
                    + " DAL_DM_JOB_SCHEDULING.USER_NAME =:USER_NAME ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", orgnId);
            selectMap.put("USER_NAME", sessionUserName);
            System.out.println("selectQuery:::" + selectQuery);
            System.out.println("selectMap:::" + selectMap);
            jobSchedularsList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (jobSchedularsList != null && !jobSchedularsList.isEmpty()) {
                jobSchedularsList = jobSchedularsList.stream()
                        .map(objArray -> {
                            try {
                                if (objArray[0] != null
                                        && objArray[0] instanceof byte[]) {
                                    objArray[0] = new RAW((byte[]) objArray[0]).stringValue();
                                }
                                if (objArray[2] != null
                                        && objArray[2] instanceof byte[]) {
                                    objArray[2] = new RAW((byte[]) objArray[2]).stringValue();
                                }
                            } catch (Exception e) {
                            }
                            return objArray;
                        }).collect(Collectors.toList());
            }

        } catch (Exception e) {
            e.printStackTrace();

        }
        return jobSchedularsList;
    }
    
    @Transactional
    public int updateActiveStatus(String orgnId,String jobId,String sessionUserName,String activeFlag) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE DAL_DM_JOB_SCHEDULING SET ACTIVE_FLAG =:ACTIVE_FLAG WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND JOB_ID =:JOB_ID ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", orgnId);
            updateMap.put("USER_NAME", sessionUserName);
            updateMap.put("JOB_ID", jobId);
            updateMap.put("ACTIVE_FLAG", activeFlag);
            System.out.println("updateQuery:::" + updateQuery);
            System.out.println("updateMap:::" + updateMap);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }
    @Transactional
    public int delteJobSchedular(String orgnId,String jobId,String sessionUserName) {
        int deleteCount = 0;
        try {
            String deleteQuery = "DELETE FROM DAL_DM_JOB_SCHEDULING WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND JOB_ID =:JOB_ID ";
            Map<String, Object> deleteMap = new HashMap<>();
            deleteMap.put("ORGN_ID", orgnId);
            deleteMap.put("USER_NAME", sessionUserName);
            deleteMap.put("JOB_ID", jobId);
            System.out.println("updateQuery:::" + deleteQuery);
            System.out.println("updateMap:::" + deleteMap);
            deleteCount = access.executeUpdateSQLNoAudit(deleteQuery, deleteMap);
            System.out.println("updateCount:::" + deleteCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return deleteCount;
    }
    
    public JSONObject insertJobs(String orgnId,
            String sessionUserName,
            String jobId,
            String jobName,
            String cronExp,
            JSONObject paramData
    ) {
        JSONObject resultObj = new JSONObject();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
            String extraParam = "";
            String extraParamPos = "";
            if (paramData != null && !paramData.isEmpty()) {
                String cronStartDate = (String) paramData.get("cronStartDate");
                String cronEndDate = (String) paramData.get("cronEndDate");
                if (cronStartDate != null
                        && !"".equalsIgnoreCase(cronStartDate)
                        && !"null".equalsIgnoreCase(cronStartDate)) {
                    extraParam += " ,SCHEDULE_START_DATE";
                    extraParamPos += " ,?";
                }
                if (cronEndDate != null
                        && !"".equalsIgnoreCase(cronEndDate)
                        && !"null".equalsIgnoreCase(cronEndDate)) {
                    extraParam += " ,SCHEDULE_END_DATE";
                    extraParamPos += " ,?";
                }
            }
            java.util.Date nextValidTime = new CronExpression(cronExp).getNextValidTimeAfter(new java.util.Date());
       	 	java.sql.Date nextValidTimeSqlDate = new java.sql.Date(nextValidTime.getTime());
            Class.forName(dataBaseDriver);
            connection = DriverManager.getConnection(dbURL, userName, password);
            String insertQuery = "INSERT INTO DAL_DM_JOB_SCHEDULING(ORGN_ID, USER_NAME, JOB_ID,"
                    + " JOB_NAME, CRON_TRIGGER, NXT_RUNNING_DATE,"
                    + "ACTIVE_FLAG, RUNNING_STATUS, JOB_INIT_PARAMS, CREATE_BY, EDIT_BY" + extraParam + ")"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?" + extraParamPos + ")";
            System.out.println("insertQuery::" + insertQuery);
            preparedStatement = connection.prepareStatement(insertQuery);
            Map<Integer, Object> insertMap = new HashMap<>();
            preparedStatement.setObject(1, orgnId);//ORGN_ID
            preparedStatement.setObject(2, sessionUserName);//USER_NAME
            preparedStatement.setObject(3, jobId);//JOB_ID
            preparedStatement.setObject(4, jobName);//JOB_NAME
            preparedStatement.setObject(5, cronExp);//CRON_TRIGGER
            preparedStatement.setObject(6, nextValidTimeSqlDate);//CRON_TRIGGER
            preparedStatement.setObject(7, "Y");//ACTIVE_FLAG
            preparedStatement.setObject(8, "STOP");//RUNNING_STATUS
            preparedStatement.setObject(9, "");//JOB_INIT_PARAMS
            preparedStatement.setObject(10, sessionUserName);//CREATE_BY
            preparedStatement.setObject(11, sessionUserName);//EDIT_BY
            int index = 11;
            if (paramData != null && !paramData.isEmpty()) {
                String cronStartDate = (String) paramData.get("cronStartDate");
                String cronEndDate = (String) paramData.get("cronEndDate");
                if (cronStartDate != null
                        && !"".equalsIgnoreCase(cronStartDate)
                        && !"null".equalsIgnoreCase(cronStartDate)) {
                    index++;
                    preparedStatement.setDate(index, new java.sql.Date(dateFormat.parse(cronStartDate).getTime()));//START_DATE
                }
                if (cronEndDate != null
                        && !"".equalsIgnoreCase(cronEndDate)
                        && !"null".equalsIgnoreCase(cronEndDate)) {
                    index++;
                    preparedStatement.setDate(index, new java.sql.Date(dateFormat.parse(cronEndDate).getTime()));//END_DATE
                }
            }
            int insertCount = preparedStatement.executeUpdate();
            if (insertCount != 0) {
                resultObj.put("flag", true);
                resultObj.put("message", "Selected job is running.");
            } else {
                resultObj.put("flag", false);
                resultObj.put("message", "Unable to run the selected job.");
            }
        } catch (SQLIntegrityConstraintViolationException ex) {
            ex.printStackTrace();
            resultObj.put("flag", false);
            resultObj.put("message", "The Selected Job is in-process,please go to the Schedule Jobs tabs and check the progress.");
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
    
    @Transactional
    public Map fetchJobScheduleInfo(String jobId) {
       
        Map jobInfoObj = new LinkedHashMap<>();
        try {
            String selectQuery = "SELECT "
            		+ "JOB_NAME, " 
            		+ "RUNNING_STATUS, "
            		+ "SCHEDULE_START_DATE, "
            		+ "SCHEDULE_END_DATE, "
            		+ "LAST_RUNNING_DATE, "
            		+ "NXT_RUNNING_DATE, "
            		+ "LAST_RUN_DURATION "
            		+ "  FROM   DAL_DM_JOB_SCHEDULING "
                    + " WHERE JOB_ID =:JOB_ID";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("JOB_ID", jobId);
            
            List<Object[]> jobInfoList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (jobInfoList != null && !jobInfoList.isEmpty()) {
            	Object [] rowData = jobInfoList.get(0);
            	jobInfoObj.put("Job Name", rowData[0]!=null ? rowData[0] : "");
            	jobInfoObj.put("Status", rowData[1]!=null ? rowData[1] : "");
            	jobInfoObj.put("Start Date", rowData[2]!=null ? String.valueOf(rowData[2]) : "");
            	jobInfoObj.put("End Date", rowData[3]!=null ? String.valueOf(rowData[3]) : "");
            	jobInfoObj.put("Last Run Date", rowData[4]!=null ? String.valueOf(rowData[4]) : "");
            	jobInfoObj.put("Next Run Date", rowData[5]!=null ? String.valueOf(rowData[5]) : "");
            	jobInfoObj.put("Last Run Duration", rowData[6]!=null ? rowData[6]+" Seconds" : "");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobInfoObj;
    }
    
  
    
    @Transactional
    public int updateRunningStatus(String jobId, String runningStatus) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE DAL_DM_JOB_SCHEDULING SET RUNNING_STATUS =:RUNNING_STATUS WHERE JOB_ID =:JOB_ID ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("JOB_ID", jobId);
            updateMap.put("RUNNING_STATUS", runningStatus);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }

    @Transactional
    public int updateLastRunningDate(String jobId, Date lastRunningDate) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE DAL_DM_JOB_SCHEDULING SET LAST_RUNNING_DATE =:LAST_RUNNING_DATE WHERE JOB_ID =:JOB_ID ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("JOB_ID", jobId);
            updateMap.put("LAST_RUNNING_DATE", lastRunningDate);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }
    @Transactional
    public int updateLastRunDuration(String jobId, long duration) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE DAL_DM_JOB_SCHEDULING SET LAST_RUN_DURATION =:LAST_RUN_DURATION WHERE JOB_ID =:JOB_ID ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("JOB_ID", jobId);
            updateMap.put("LAST_RUN_DURATION", duration);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }
    
    @Transactional
    public int updateNextRunningDate(String jobId, Date nextRunningDate) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE DAL_DM_JOB_SCHEDULING SET NXT_RUNNING_DATE =:NXT_RUNNING_DATE WHERE JOB_ID =:JOB_ID ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("JOB_ID", jobId);
            updateMap.put("NXT_RUNNING_DATE", nextRunningDate);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }
    @Transactional
    public int updateJobStartDate(String jobId, Date jobStartDate) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE DAL_DM_JOB_SCHEDULING SET JOB_START_DATE =:JOB_START_DATE WHERE JOB_ID =:JOB_ID ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("JOB_ID", jobId);
            updateMap.put("JOB_START_DATE", jobStartDate);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }
    
    
}
