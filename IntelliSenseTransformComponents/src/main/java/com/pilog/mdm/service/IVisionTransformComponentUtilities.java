/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.opencsv.CSVReader;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;

import com.sap.mw.jco.IRepository;
import com.sap.mw.jco.JCO;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Ravindar.P
 */
@Service
public class IVisionTransformComponentUtilities {

    private IRepository theRepository;
    @Value("${jdbc.driver}")
    private String dataBaseDriver;

    @Value("${jdbc.username}")
    private String userName;

    @Value("${jdbc.password}")
    private String password;

    @Value("${jdbc.url}")
    private String dbURL;

    @Autowired
    private DataAccess access;

    @Value("${jdbc.accessName}")
    private String accessName;

    @Transactional
    public void dropStagingTable(String tableName, Connection connection) {
        PreparedStatement preparedStatement = null;
        Boolean tableDroped = false;
        try {
            String dropQuery = "DROP TABLE " + tableName;

            preparedStatement = connection.prepareStatement(dropQuery);
            tableDroped = preparedStatement.execute();
            System.out.println(tableName + " droped ");
        } catch (Exception e) {
            System.out.println(tableName + " drop failed ");
            e.printStackTrace();
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }
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
    public void deleteProcessBarlog(String userName, String orgnId, String jobId) {
        try {
            String deleteQuery = "DELETE FROM RECORD_DM_PROCESS_BAR"
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
    public void deleteJobProcessSteps(String userName, String orgnId, String jobId) {
        try {
            String deleteQuery = "DELETE FROM RECORD_DM_JOB_PROCESS_STEPS "
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
    public void deleteJobPreview(String userName, String orgnId, String jobId) {
        try {
            String deleteQuery = "DELETE FROM C_ETL_JOB_PREVIEW "
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
    public int getLastStepsSequenceNo(String userName, String orgnId) {
        int lastSequenceNo = 0;
        try {
            Map<String, Object> selectMap = new HashMap<>();
            String selectQuery = "SELECT MAX(SEQUENCE_NO) FROM RECORD_DM_JOB_PROCESS_STEPS"
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
    public int getLastProcessBarLogSequenceNo(String userName, String orgnId) {
        int lastSequenceNo = 0;
        try {
            Map<String, Object> selectMap = new HashMap<>();
            String selectQuery = "SELECT MAX(SEQUENCE_NO) FROM RECORD_DM_PROCESS_BAR"
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
    public void processETLLog(String sessionUserName,
                              String orgnId,
                              String logTxt,
                              String logType,
                              int sequenceNo,
                              String processFlag,
                              String jobId
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {
            if (logTxt != null
                    && !"".equalsIgnoreCase(logTxt)
                    && !"null".equalsIgnoreCase(logTxt)
                    && logTxt.length() > 3000) {
                logTxt = logTxt.substring(0, 3000);
            }
            int lastSequenceNo = getLastLogSequenceNo(sessionUserName, orgnId);
            lastSequenceNo++;
            String insertQuery = "INSERT INTO RECORD_DM_PROCESS_LOG (SEQUENCE_NO, USER_NAME, ORGN_ID, LOG_TXT,LOG_TYPE,PROCESS_FLAG,JOB_ID)"
                    + " VALUES (?,?,?,?,?,?,?)";
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
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

    @Transactional
    public void processETLLogSteps(String sessionUserName,
                                   String orgnId,
                                   String stepOperator,
                                   String stepType,
                                   String stepStatus,
                                   String processFlag,
                                   String jobId
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {

            int lastSequenceNo = getLastStepsSequenceNo(sessionUserName, orgnId);
            lastSequenceNo++;
            String insertQuery = "INSERT INTO RECORD_DM_JOB_PROCESS_STEPS (SEQUENCE_NO, USER_NAME, ORGN_ID, STEP_OPERATOR,STEP_TYPE,PROCESS_FLAG,STEP_STATUS,JOB_ID)"
                    + " VALUES (?,?,?,?,?,?,?,?)";
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(insertQuery);
            logStmt.setObject(1, lastSequenceNo);//SEQUENCE_NO
            logStmt.setObject(2, sessionUserName);//USER_NAME
            logStmt.setObject(3, orgnId);//ORGN_ID
            logStmt.setObject(4, stepOperator);//LOG_TXT
            logStmt.setObject(5, stepType);//LOG_TYPE
            logStmt.setObject(6, processFlag);//PROCESS_FLAG
            logStmt.setObject(7, stepStatus);//PROCESS_FLAG
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

    @Transactional
    public void processBarETLLog(String sessionUserName,
                                 String orgnId,
                                 String logTxt,
                                 int sequenceNo,
                                 String processFlag,
                                 String jobId,
                                 String stepFlag,
                                 String stepCompletionTime,
                                 String stepOperatorId
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {
            if (logTxt != null
                    && !"".equalsIgnoreCase(logTxt)
                    && !"null".equalsIgnoreCase(logTxt)
                    && logTxt.length() > 3000) {
                logTxt = logTxt.substring(0, 3000);
            }
            int lastSequenceNo = getLastProcessBarLogSequenceNo(sessionUserName, orgnId);
            lastSequenceNo++;
            String insertQuery = "INSERT INTO RECORD_DM_PROCESS_BAR ("
                    + "SEQUENCE_NO, " //1
                    + "USER_NAME, "//2
                    + "ORGN_ID, "//3
                    + "LOG_TXT, "//4
                    + "PROCESS_FLAG, "//5
                    + "STEP_COMPLETE_FLAG, "//6
                    + "STEP_COMPLETION_TIME, "//7
                    + "STEP_OPERATOR, "//8
                    + "JOB_ID)"//9
                    + " VALUES (?,?,?,?,?,?,?,?,?)";
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(insertQuery);
            logStmt.setObject(1, lastSequenceNo);//SEQUENCE_NO
            logStmt.setObject(2, sessionUserName);//USER_NAME
            logStmt.setObject(3, orgnId);//ORGN_ID
            logStmt.setObject(4, logTxt);//PROCESS_FLAG
            logStmt.setObject(5, processFlag);//PROCESS_FLAG
            logStmt.setObject(6, stepFlag);//PROCESS_FLAG
            logStmt.setObject(7, stepCompletionTime);//PROCESS_FLAG
            logStmt.setObject(8, stepOperatorId);//PROCESS_FLAG
            logStmt.setObject(9, jobId);//JOB_ID
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

    @Transactional
    public void processETLReconciliation(
            String sessionUserName,
            String orgnId,
            String jobId,
            String sourceTable,
            String sourceCount,
            String targetTable,
            String targetTableCount,
            String rejectedRecords,
            String comments,
            String subJobId,
            String srcColumnCount,
            String mappedColumnCount
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {

            String insertQuery = "INSERT INTO C_ETL_RECONCILIATION ("
                    + "USER_NAME, "//3
                    + "ORGN_ID, "//1
                    + "JOB_ID, "//2
                    + "SOURCE_TABLE, "//4
                    + "SOURCE_COUNT, "//5
                    + "TARGET_TABLE, "//6
                    + "PROCESSED_RECORDS_COUNT, "//7
                    + "REJECTED_RECORDS_COUNT, "//8
                    + "COMMENTS, "//9
                    + "SUB_JOB_ID, "//10
                    + "SOURCE_COLUMN_COUNT, "//11
                    + "MAPPED_COLUMN_COUNT "//12
                    + ")"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
            sourceTable = (sourceTable!=null && sourceTable.contains(".")) ? sourceTable.split("\\.")[1] : sourceTable;
            targetTable = (targetTable!=null && targetTable.contains(".")) ? targetTable.split("\\.")[1] : targetTable;
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(insertQuery);
            logStmt.setObject(1, sessionUserName);//orgnId
            logStmt.setObject(2, orgnId);//sessionUserName
            logStmt.setObject(3, jobId);//orgnId
            logStmt.setObject(4, sourceTable);//sourceTable
            logStmt.setObject(5, sourceCount);//sourceCount
            logStmt.setObject(6, targetTable);//targetTable
            logStmt.setObject(7, targetTableCount);//targetTableCount
            logStmt.setObject(8, rejectedRecords);//rejectedRecords
            logStmt.setObject(9, comments);//comments
            logStmt.setObject(10, subJobId);//comments
            logStmt.setObject(11, srcColumnCount);//comments
            logStmt.setObject(12, mappedColumnCount);//comments

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


    @Transactional
    public void processETLReconciliationDelete(
            String sessionUserName,
            String orgnId,
            String jobId,
            String subJobId
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {

            String deleteQuery = "DELETE FROM C_ETL_RECONCILIATION "
                    + " WHERE ORGN_ID=? "//1
                    + " AND USER_NAME=?" //2
                    + " AND JOB_ID=?"
                    + " AND SUB_JOB_ID=?"; //3

            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(deleteQuery);
            logStmt.setObject(1, orgnId);
            logStmt.setObject(2, sessionUserName);
            logStmt.setObject(3, jobId);
            logStmt.setObject(4, subJobId);

            int deleteCount = logStmt.executeUpdate();
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


    @Transactional
    public void processETLReconciliationUpdateTarget(
            String sessionUserName,
            String orgnId,
            String jobId,
            String targetTable,
            String targetTableCount,
            String subJobId,
            String mappedColumnsCount
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {

            String updateQuery = "UPDATE C_ETL_RECONCILIATION SET "
                    + "TARGET_TABLE=?, " //1
                    + "PROCESSED_RECORDS_COUNT=?, "//2
                    + " MAPPED_COLUMN_COUNT=?" //3
                    + " WHERE ORGN_ID=? "//4
                    + " AND USER_NAME=?" //5
                    + " AND JOB_ID=?"//6
                    + " AND SUB_JOB_ID=?"; //7

            targetTable = (targetTable!=null && targetTable.contains(".")) ? targetTable.split("\\.")[1] : targetTable;

            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(updateQuery);
            logStmt.setObject(1, targetTable);
            logStmt.setObject(2, targetTableCount);
            logStmt.setObject(3, mappedColumnsCount);
            logStmt.setObject(4, orgnId);
            logStmt.setObject(5, sessionUserName);
            logStmt.setObject(6, jobId);
            logStmt.setObject(7, subJobId);


            int updateCount = logStmt.executeUpdate();
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

    @Transactional
    public void processETLReconciliationUpdateRejectRecords(
            String sessionUserName,
            String orgnId,
            String jobId,
            String rejectedRecords,
            String subJobId
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {

            String updateQuery = "UPDATE C_ETL_RECONCILIATION SET "
                    + "REJECTED_RECORDS_COUNT=? " //1
                    + " WHERE ORGN_ID=? "//2
                    + " AND USER_NAME=?" //3
                    + " AND JOB_ID=?"
                    + " AND SUB_JOB_ID=?"; //3

            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(updateQuery);
            logStmt.setObject(1, rejectedRecords);
            logStmt.setObject(2, orgnId);
            logStmt.setObject(3, sessionUserName);
            logStmt.setObject(4, jobId);
            logStmt.setObject(5, subJobId);


            int updateCount = logStmt.executeUpdate();
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

    @Transactional
    public void processETLReconciliationUpdateComments(
            String sessionUserName,
            String orgnId,
            String jobId,
            String comments,
            String subJobId
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {

            String updateQuery = "UPDATE C_ETL_RECONCILIATION SET "
                    + "COMMENTS=? " //1
                    + " WHERE ORGN_ID=? "//2
                    + " AND USER_NAME=?" //3
                    + " AND JOB_ID=?"
                    + " AND SUB_JOB_ID=?"; //4

            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(updateQuery);
            logStmt.setObject(1, comments);
            logStmt.setObject(2, orgnId);
            logStmt.setObject(3, sessionUserName);
            logStmt.setObject(4, jobId);
            logStmt.setObject(5, subJobId);

            int updateCount = logStmt.executeUpdate();
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


    @Transactional
    public List getETLJobPreviewCount(
            String sessionUserName,
            String orgnId,
            String jobId
    ) {
        List dataList = new ArrayList();
        try {

            String selectQuery = "SELECT "
                    + "ORGN_ID, "
                    + "JOB_ID, "
                    + "USER_NAME, "
                    + "OPERATOR_ID, "
                    + "OPERATOR_COUNT, "
                    + "OPERATOR_TITLE "
                    + "FROM C_ETL_JOB_PREVIEW "
                    + " WHERE ORGN_ID=:ORGN_ID"
                    + " AND JOB_ID=:JOB_ID"
                    + " AND USER_NAME=:USER_NAME";

            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("USER_NAME", sessionUserName);
            selectMap.put("ORGN_ID", orgnId);
            selectMap.put("JOB_ID", jobId);

            dataList = access.sqlqueryWithParams(selectQuery, selectMap);

//            System.out.println("insert Log Count " + insertCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataList;
    }

    @Transactional
    public List getETLJObReconciliation(
            String sessionUserName,
            String orgnId,
            String jobId,
            String subjobId
    ) {
        List dataList = new ArrayList();
        try {

            String selectQuery = "SELECT "
                    + "ORGN_ID, "//0
                    + "JOB_ID, "//1
                    + "USER_NAME, "//2
                    + "SOURCE_TABLE, "//3
                    + "SOURCE_COUNT, "//4
                    + "TARGET_TABLE, "//5
                    + "PROCESSED_RECORDS_COUNT, "//6
                    + "REJECTED_RECORDS_COUNT, "//7
                    + "COMMENTS "//8
                    + " FROM C_ETL_RECONCILIATION "
                    + " WHERE ORGN_ID=:ORGN_ID"
                    + " AND JOB_ID=:JOB_ID"
                    + " AND USER_NAME=:USER_NAME"
                    + " AND SUB_JOB_ID=:SUB_JOB_ID";

            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("USER_NAME", sessionUserName);
            selectMap.put("ORGN_ID", orgnId);
            selectMap.put("JOB_ID", jobId);
            selectMap.put("SUB_JOB_ID", subjobId);

            dataList = access.sqlqueryWithParams(selectQuery, selectMap);

//            System.out.println("insert Log Count " + insertCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataList;
    }


    @Transactional
    public List getColumnReconciliation(HttpServletRequest request, String jobId, String sourceTable,
                                        String targetTable, JSONObject targetTrfmRules, JSONObject sourceConnObj, JSONObject targetConnObj,
                                        String sessionUserName, String orgnId, String subJobId) {
        List localDataList = new ArrayList();
        List dataList = new ArrayList();
        JCO.Client sapConn = null;
        Connection sourceConn = null;
        Connection targetConn = null;
        try {
            List fromColumnsList = new ArrayList();
            List toColumnsList = new ArrayList();
            if (targetTrfmRules != null && !targetTrfmRules.isEmpty()) {

                JSONArray colMappingsData = (JSONArray) targetTrfmRules.get("colMappingsData");

                for (int i = 0; i < colMappingsData.size(); i++) {
                    JSONObject rowData = (JSONObject) colMappingsData.get(i);
                    String destinationColumn = (String) rowData.get("destinationColumn");
                    if (destinationColumn != null && destinationColumn.contains(":")) {
                        destinationColumn = destinationColumn.split(":")[1];
                    }

                    String sourceColumn = (String) rowData.get("sourceColumnActualValue");
                    if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
                        if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
                            sourceColumn = sourceColumn.split(":")[1];
                        }
                        fromColumnsList.add(sourceColumn);
                    }
                    String sourceTableStr = (String) rowData.get("sourceTable");

                    String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
                    String columnClause = (String) rowData.get("columnClauseActualValue");

                    if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {

                    } else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
                        sourceColumn = "'" + defaultValue + "'";
                        fromColumnsList.add(sourceColumn);
                    } else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
                        String funcolumnslistStr = (String) rowData.get("funcolumnslist");
                        JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
                        sourceColumn = columnClause;
                        sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
                        fromColumnsList.add(sourceColumn);
                    }

                    toColumnsList.add(destinationColumn);
                }
            }

            String selectQuery = "SELECT SOURCE_TABLE, TARGET_TABLE, PROCESSED_RECORDS_COUNT, UPDATE_COUNT_INFO  FROM C_ETL_RECONCILIATION "
                    + " WHERE JOB_ID='"+jobId+"' AND USER_NAME='"+sessionUserName+"' AND ORGN_ID = '"+orgnId+"' AND SUB_JOB_ID='"+subJobId+"' ";

            Map<String, Object> selectMap = new HashMap<>();

            String insertCount = "";
            String updateCount = "";
            JSONObject compDesStringJson = new JSONObject();
            dataList = access.sqlqueryWithParams(selectQuery, selectMap);
            if (dataList != null && !dataList.isEmpty()) {
                Object[] rowData = (Object[]) dataList.get(0);
                insertCount = rowData[2] != null ? (String) rowData[2] : "";
                Clob colUpdateCount = rowData[3] != null ? (Clob) rowData[3] : null;
                String compDesString = new PilogUtilities().clobToString(colUpdateCount);
                if (!"".equalsIgnoreCase(compDesString)) {
                    JSONParser parser = new JSONParser();
                    compDesStringJson = (JSONObject) parser.parse(compDesString);
                }
                updateCount = "0";
            }
            List<Object[]> sourceTableMetaData =  new ArrayList();
            Object sourceConnection = getConnection(sourceConnObj);
            if (sourceConnection instanceof Connection) {
                sourceConn = (Connection)sourceConnection;
                sourceTableMetaData = getTreeDMTableColumnsOpt(sourceConn, request, sourceConnObj, sourceTable);
            } else if (sourceConnection instanceof JCO.Client) {
                sapConn = (JCO.Client) sourceConnection;
                sourceTableMetaData = getSAPTableColumnsWithType(request, sapConn, sourceTable);
            }

            targetConn = (Connection)getConnection(targetConnObj);
            List<Object[]> targetTableMetaData = getTreeDMTableColumnsOpt(targetConn, request, targetConnObj, targetTable);

            JSONObject sourceDataObj = new JSONObject();
            JSONObject targetDataObj = new JSONObject();
            for (int i=0; i< sourceTableMetaData.size(); i++) {
                Object [] rowdata = sourceTableMetaData.get(i);
                int addVal=0;
                if (sourceConnection instanceof JCO.Client) {
                    addVal = 1;
                }
                String column = (String)rowdata[1+addVal];

                if (fromColumnsList.contains(column)) {
                    JSONObject obj = new JSONObject();
                    obj.put("column", (String)rowdata[1 + addVal]);
                    obj.put("datatype", (String)rowdata[2 + addVal]);
                    obj.put("length", (String)rowdata[3 + addVal]);
                    sourceDataObj.put(column, obj);
                }
            }
            for (int i=0; i< targetTableMetaData.size(); i++) {
                Object [] rowdata = targetTableMetaData.get(i);
                String column = (String)rowdata[1];
                if (toColumnsList.contains(column)) {
                    JSONObject obj = new JSONObject();
                    obj.put("column", (String)rowdata[1]);
                    obj.put("datatype", (String)rowdata[2]);
                    obj.put("length", (String)rowdata[3]);
                    targetDataObj.put(column, obj);
                }
            }

            for (int i=0; i<toColumnsList.size();i++) {
                JSONObject rowData = new JSONObject();
                String targetColumn = (String)toColumnsList.get(i);
                String sourceColumn = (String)fromColumnsList.get(i);
                JSONObject srcObj = sourceDataObj.get(sourceColumn)!=null ? (JSONObject)sourceDataObj.get(sourceColumn) : new JSONObject();
                JSONObject trgObj = targetDataObj.get(targetColumn)!=null ? (JSONObject)targetDataObj.get(targetColumn) : new JSONObject();
                rowData.put("SOURCE_COLUMN",sourceColumn);
                rowData.put("SOURCE_DATATYPE",srcObj.get("datatype"));
                rowData.put("SOURCE_LENGTH",srcObj.get("length"));
                rowData.put("TARGET_COLUMN",targetColumn);
                rowData.put("TARGET_DATATYPE",trgObj.get("datatype"));
                rowData.put("TARGET_LENGTH",trgObj.get("length"));

                rowData.put("INSERT_COUNT", insertCount);
                //281222
                if (compDesStringJson.containsKey(targetColumn)) {
                    updateCount = compDesStringJson.get(targetColumn).toString();
                } else {
                    updateCount = "";
                }
                rowData.put("UPDATE_COUNT", updateCount);

                localDataList.add(rowData);
            }


//            System.out.println("insert Log Count " + insertCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                sourceConn.close();
                targetConn.close();
            }catch(Exception e) {}

        }
        return localDataList;
    }

    public Map sortOperators(Map operators) {
        Map sortedOperators = new LinkedHashMap();
        Map sortedSequenceOperatorMap = new LinkedHashMap();
        int[] keyArray = new int[operators.size()];
        int[] sequenceArray = new int[operators.size()];
        int[] sortedKeyArray = new int[operators.size()];
        int[] sortedSequenceArray = new int[operators.size()];

        try {
            int itr = 0;
            for (Object key : operators.keySet()) {
                JSONObject operator = (JSONObject) operators.get(key);
                String executionSequence = String.valueOf(operator.get("executionSequence"));
                sortedSequenceOperatorMap.put(key, executionSequence);
                sequenceArray[itr] = Integer.valueOf(executionSequence);
                keyArray[itr] = Integer.valueOf(String.valueOf(key));
                itr++;
            }

            for (int i = 0; i < sequenceArray.length; i++) {
                for (int j = i + 1; j < sequenceArray.length; j++) {
                    int temp = 0;
                    int tempkey = 0;
                    if (sequenceArray[i] > sequenceArray[j]) {
                        temp = sequenceArray[i];
                        sequenceArray[i] = sequenceArray[j];
                        sequenceArray[j] = temp;

                        tempkey = keyArray[i];
                        keyArray[i] = keyArray[j];
                        keyArray[j] = tempkey;
                    }
                }
            }

            for (int i = 0; i < keyArray.length; i++) {
                String key = String.valueOf(keyArray[i]);
                sortedOperators.put(key, operators.get(key));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sortedOperators;
    }

    //        public Map sortOperators(Map operators) {
//        Map sortedOperators = new LinkedHashMap();
//        try {
//            ArrayList sortedKeyList = (ArrayList) operators.keySet().stream().map(e -> Integer.valueOf((String) e)).collect(Collectors.toList());
//            Collections.sort(sortedKeyList);
//            for (int i = 0; i < sortedKeyList.size(); i++) {
//                String sortedKey = String.valueOf(sortedKeyList.get(i));
//                sortedOperators.put(sortedKey, operators.get(sortedKey));
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return sortedOperators;
//    }
    public Map sortGroupJobOperators(Map operators) {
        Map sortedOperators = new LinkedHashMap();
        try {
            ArrayList sortedKeyList = (ArrayList) operators.keySet().stream().map(opdata -> Integer.valueOf(((JSONObject) opdata).get("mapExecutionSeqNo") != null ? (String) ((JSONObject) opdata).get("mapExecutionSeqNo") : "999")).collect(Collectors.toList());
            Collections.sort(sortedKeyList);
            for (int i = 0; i < sortedKeyList.size(); i++) {
                String sortedKey = String.valueOf(sortedKeyList.get(i));
                sortedOperators.put(sortedKey, operators.get(sortedKey));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sortedOperators;
    }

    public Boolean createTable(String tableName, List columnsList, List dataTypeList, Connection connection) {
        Boolean tableCreated = false;
        PreparedStatement preparedStatement = null;

        try {

            String query = "CREATE TABLE " + tableName + " (";
            for (int i = 0; i < columnsList.size(); i++) {
                String columnName = String.valueOf(columnsList.get(i));
                if (columnName.contains("/")){
                    columnName ="\""+columnName+"\"";
                }
                if ((i + 1) != columnsList.size()) {
                    query += columnName + " " + dataTypeList.get(i) + ", ";
                } else {
                    query += columnName + " " + dataTypeList.get(i) + " ";
                }
            }
            query += ")";
            preparedStatement = connection.prepareStatement(query);
            tableCreated = preparedStatement.execute();

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return tableCreated;
    }

    public Boolean createTableWithQuery(String query, Connection connection) {
        Boolean tableCreated = false;
        PreparedStatement preparedStatement = null;

        try {

            preparedStatement = connection.prepareStatement(query);
            tableCreated = preparedStatement.execute();

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }


            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return tableCreated;
    }

    public Connection getCurrentConnection() {
        Connection connection = null;
        try {
            Class.forName(dataBaseDriver);
            connection = DriverManager.getConnection(dbURL, userName, password);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
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

    public List<Object[]> getTableColumnsOpt(Connection connection, JSONObject dbObj, String tableName) {
        List<Object[]> columnsList = new ArrayList<>();
        try {
            if (connection != null) {
                if (tableName != null && tableName.contains(",")) {
                    tableName = tableName.replaceAll(",", "','");
                }
                String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
                String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "TableColumns";
                System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
                Class clazz = Class.forName(initParamClassName);
                Class<?>[] paramTypes = {Connection.class, String.class, String.class};
                Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
                Object targetObj = new PilogUtilities().createObjectByClass(clazz);
                columnsList = (List<Object[]>) method.invoke(targetObj, connection, String.valueOf(dbObj.get("CONN_DB_NAME")), tableName);
            }

        } catch (Exception e) {

            e.printStackTrace();
        }
        return columnsList;
    }

    public List getHeaderDataTypesOfImportedFile(HttpServletRequest request, String filePath) {
        List<String> headerDataTypes = new ArrayList();
        try {
            if (filePath != null && !"".equalsIgnoreCase(filePath) && !"null".equalsIgnoreCase(filePath)) {
                String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());

                if (fileExt != null && !"".equalsIgnoreCase(fileExt)) {
                    if ("txt".equalsIgnoreCase(fileExt)
                            || "csv".equalsIgnoreCase(fileExt)
                            || "json".equalsIgnoreCase(fileExt)) {

                        CsvParserSettings settings = new CsvParserSettings();
                        settings.detectFormatAutomatically();

                        CsvParser parser = new CsvParser(settings);
                        List<String[]> rows = parser.parseAll(new File(filePath));

                        // if you want to see what it detected
//                        CsvFormatDetector formatdetect =  new CsvFormatDetector();
                        char columnSeparator = ',';
                        String fileType = request.getParameter("fileType");
//                        char columnSeparator = '\t';
//                        char columnSeparator = ',';
                        if (!(fileType != null
                                && !"".equalsIgnoreCase(fileType)
                                && !"null".equalsIgnoreCase(fileType))) {
                            fileType = (String) request.getAttribute("fileType");
                        }
                        if (".json".equalsIgnoreCase(fileType)) {
                            columnSeparator = ',';
                        } else {
                            CsvFormat format = parser.getDetectedFormat();
                            columnSeparator = format.getDelimiter();
                        }

                        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"), columnSeparator);

                        String[] nextLine;
                        reader.readNext();
                        List<String> values = null;
                        while ((nextLine = reader.readNext()) != null) {
                            if (nextLine.length != 0 && nextLine[0].contains("" + columnSeparator)) {
                                values = new ArrayList<>(Arrays.asList(nextLine[0].split("" + columnSeparator)));
                            } else {
                                values = new ArrayList<>(Arrays.asList(nextLine));
                            }

                            break;
                        }
                        for (int i = 0; i < values.size(); i++) {
                            String value = values.get(i);

                            int dataTypeLength = value.length();
                            String dataType = getOracleDataTypeOfValue(value, dataTypeLength);
                            headerDataTypes.add(dataType);
                        }

                    } else if ("xls".equalsIgnoreCase(fileExt) || "xlsx".equalsIgnoreCase(fileExt)) {
                        headerDataTypes = new ArrayList<>();
                        Workbook workBook = null;
                        Sheet sheet = null;

                        //PKH sheet Header
                        String sheetNum = request.getParameter("sheetNo");
                        int sheetNo = (sheetNum != null && !"".equalsIgnoreCase(sheetNum)) ? (Integer.parseInt(sheetNum)) : 0;
                        //PKH sheet Header

//                        if (fileExt != null && "xls".equalsIgnoreCase(fileExt)) { // commented by PKH
//                            workBook = WorkbookFactory.create(new File(filePath));
//                            sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
//                        } else {
                        System.out.println("Before::::" + new Date());
//                fis = new FileInputStream(new File(filepath));              
//                XSSFWorkbook xssfWb = (XSSFWorkbook) new XSSFWorkbook(fis);
                        workBook = WorkbookFactory.create(new File(filePath));
                        System.out.println("After::fileInputStream::" + new Date());
                        if (workBook.getSheetAt(sheetNo) instanceof XSSFSheet) {
                            sheet = (XSSFSheet) workBook.getSheetAt(sheetNo);
                        } else if (workBook.getSheetAt(sheetNo) instanceof HSSFSheet) {
                            sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
                        }
//                sheet = (XSSFSheet) xssfWb.getSheetAt(0);
//                        }
                        if (sheet != null) {
                            Row columnCountrow = sheet.getRow(0);
                            int columnCount = columnCountrow.getLastCellNum();
                            Row row = sheet.getRow(1);
                            if (row != null) {
                                for (int j = 0; j < columnCount; j++) {
                                    //  System.out.println("Cell Num:::" + j + ":::Start Date And Time :::" + new Date());

                                    try {
                                        Cell cell = row.getCell(j);
                                        // String value = cell.getStringCellValue();
                                        // int dataTypeLength = value.length();
                                        // String dataType = getOracleDataTypeOfValue(value, dataTypeLength);
                                        // headerDataTypes.add(dataType);
                                        if (cell != null) {

                                            switch (cell.getCellType()) {
                                                case Cell.CELL_TYPE_STRING:
                                                    headerDataTypes.add("VARCHAR2(4000)");
                                                    break;
                                                case Cell.CELL_TYPE_BOOLEAN:
                                                    headerDataTypes.add("BOOLEAN");
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
                                                    break;
                                                case Cell.CELL_TYPE_NUMERIC:

                                                    if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                                        headerDataTypes.add("DATE");
//
                                                    } else {
                                                        double numericValue = cell.getNumericCellValue();
                                                        if (numericValue == Math.floor(numericValue)) {
                                                            headerDataTypes.add("NUMBER"); // or "INTEGER" if you prefer
                                                        } else {
                                                            headerDataTypes.add("FLOAT"); // or "NUMBER(p, s)" with precision and scale if needed
                                                        }
                                                    }
                                                    break;
                                                case Cell.CELL_TYPE_BLANK:
                                                    headerDataTypes.add("VARCHAR2(4000)");
                                                    break;
                                            }
                                        } else {
                                            headerDataTypes.add("VARCHAR2(4000)");
//                            testMap.put(stmt, "");
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        headerDataTypes.add("VARCHAR2(100)");
                                        continue;
                                    }

                                }// end of row cell loop
                            }
                        }

                    } else if ("xml".equalsIgnoreCase(fileExt)) {
                        headerDataTypes = new ArrayList<>();
                        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                        DocumentBuilder builder = factory.newDocumentBuilder();
                        Document document = builder.parse(new File(filePath));
                        document.getDocumentElement().normalize();
                        Element root = document.getDocumentElement();
                        if (root.hasChildNodes() && root.getChildNodes().getLength() > 1) {
                            String evaluateTagName = "/" + root.getTagName();
                            NodeList rootList = root.getChildNodes();
                            if (!"#Text".equalsIgnoreCase(rootList.item(0).getNodeName())) {
                                evaluateTagName += "/" + rootList.item(0).getNodeName();
                            } else {
                                evaluateTagName += "/" + rootList.item(1).getNodeName();
                            }

                            System.out.println("evaluateTagName:::" + evaluateTagName);
                            XPath xpath = XPathFactory.newInstance().newXPath();
                            NodeList dataNodeList = (NodeList) xpath.evaluate(evaluateTagName,
                                    //            NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
                                    document,
                                    XPathConstants.NODESET);
                            if (dataNodeList != null && dataNodeList.getLength() != 0) {
                                int rowCount = dataNodeList.getLength();
//                                Node node = dataNodeList.item(0);
//                                if (node.getNodeType() == Node.ELEMENT_NODE) {
//                                    NodeList childNodeList = node.getChildNodes();
//                                    for (int i = 0; i < childNodeList.getLength(); i++) {// Columns
//
//                                        Node childNode = childNodeList.item(i);
//                                        if (childNode != null
//                                                && !"#Text".equalsIgnoreCase(childNode.getNodeName())) {
//                                            headerDataTypes.add(childNode.getNodeName());
//                                            System.err.println(childNode.getNodeName() + "---> " + childNode.getTextContent());
//                                        }
//
//                                    }// end of columns loop
//
//                                }

                                Node node = dataNodeList.item(1);

                                if (node != null && node.getNodeType() == Node.ELEMENT_NODE) {
//                            JSONObject dataObject = new JSONObject();

                                    // dataObject.put("totalrecords", rowCount);
                                    NodeList childNodeList = node.getChildNodes();
                                    Object[] dataObject = new Object[childNodeList.getLength()];
                                    for (int j = 0; j < childNodeList.getLength(); j++) {
                                        try {
                                            Node childNode = childNodeList.item(j);
//                                    int childNodeIndex = j;
//                                    int nodeListLength = childNodeList.getLength();
                                            if (childNode != null) {
                                                if (childNode != null
                                                        && childNode.getNodeType() == Node.ELEMENT_NODE) {
                                                    try {
                                                        if (childNode.getTextContent() != null
                                                                && !"".equalsIgnoreCase(childNode.getTextContent())
                                                                && !"null".equalsIgnoreCase(childNode.getTextContent())) {
//                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), childNode.getTextContent());
                                                            String value = childNode.getTextContent();
                                                            int dataTypeLength = value.length();
                                                            String dataType = getOracleDataTypeOfValue(value, dataTypeLength);
                                                            headerDataTypes.add(dataType);

                                                        } else {
//                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), "");
                                                            headerDataTypes.add("VARCHAR2(100)");
                                                        }

                                                    } catch (Exception e) {
                                                        headerDataTypes.add("VARCHAR2(100)");
                                                        continue;
                                                    }
                                                    // Need to set the Data

                                                }
                                            }
                                        } catch (Exception e) {

                                            continue;
                                        }

                                    }// column list loop

                                }

                            }

                        }
                    } else if ("pdf".equalsIgnoreCase(fileExt)) {
                        headerDataTypes = new ArrayList();
                        String result = readPDFRestApi(request, filePath);
                        JSONObject apiPdfJsonData = (JSONObject) JSONValue.parse(result);
                        List<String> headerArray = (List<String>) apiPdfJsonData.get("columns");
                        List<List<String>> resultArray = (List<List<String>>) apiPdfJsonData.get("data");
                        if (resultArray != null && !resultArray.isEmpty()) {
                            LinkedHashMap rowObj = (LinkedHashMap) resultArray.get(0);
//                            Iterator itr = rowObj.values().iterator();
                            for (int i = 0; i < rowObj.size(); i++) {
                                headerDataTypes.add("VARCHAR2(4000)");
                            }

                        }

                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return headerDataTypes;
    }

    public List getHeadersOfImportedFile(HttpServletRequest request, String filePath) {
        List<String> headers = null;
        Map<String, Integer> headerCount = new HashMap();
        try {
            if (filePath != null && !"".equalsIgnoreCase(filePath) && !"null".equalsIgnoreCase(filePath)) {
                String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());

                if (fileExt != null && !"".equalsIgnoreCase(fileExt)) {
                    if ("txt".equalsIgnoreCase(fileExt)
                            || "csv".equalsIgnoreCase(fileExt)) {

                        CsvParserSettings settings = new CsvParserSettings();
                        settings.detectFormatAutomatically();

                        CsvParser parser = new CsvParser(settings);
                        List<String[]> rows = parser.parseAll(new File(filePath));

                        // if you want to see what it detected
//                        CsvFormatDetector formatdetect =  new CsvFormatDetector();
                        CsvFormat format = parser.getDetectedFormat();
                        char columnSeparator = format.getDelimiter();

                        String fileType = request.getParameter("fileType");
//                        char columnSeparator = '\t';
//                        char columnSeparator = ',';
                        if (!(fileType != null
                                && !"".equalsIgnoreCase(fileType)
                                && !"null".equalsIgnoreCase(fileType))) {
                            fileType = (String) request.getAttribute("fileType");
                        }
                        if (".json".equalsIgnoreCase(fileType)) {
                            columnSeparator = ',';
                        }
//                        FileInputStream fis = new FileInputStream("c:/temp/utf8.txt");
//                        BufferedReader r = new BufferedReader(new InputStreamReader(fis,
//                                "UTF8"));
//                        CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
//
//                        // create csvReader object with parameter
//                        // filereader and parser
//                        CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
//                                .withCSVParser(parser)
//                                .build();
                        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF8"), columnSeparator);

                        String[] nextLine;
                        while ((nextLine = reader.readNext()) != null) {
                            for (String header : nextLine) {
                                if (headerCount.containsKey(header.toLowerCase())) {
                                    int count = headerCount.get(header.toLowerCase());
                                    count++;
                                    headerCount.put(header.toLowerCase(), count);
                                    if (count == 2) {
                                        header = header + "_1";
                                    } else {
                                        header = header + "_" + (count - 1);
                                    }
                                } else {
                                    headerCount.put(header.toLowerCase(), 1);
                                }
                                headers.add(header);
                            }
                            break;
                        }
                    } else if ("xls".equalsIgnoreCase(fileExt) || "xlsx".equalsIgnoreCase(fileExt)) {
                        headers = new ArrayList<>();
                        //Workbook workBook = null;
                        Sheet sheet = null;

                        //PKH sheet Header
                        String sheetNum = request.getParameter("sheetNo");
                        int sheetNo = (sheetNum != null && !"".equalsIgnoreCase(sheetNum)) ? (Integer.parseInt(sheetNum)) : 0;
                        //PKH sheet Header

//                        if (fileExt != null && "xls".equalsIgnoreCase(fileExt)) { // commented by PKH
//                            workBook = WorkbookFactory.create(new File(filePath));
//                            sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
//                        } else {
                        System.out.println("Before::::" + new Date());
//                fis = new FileInputStream(new File(filepath));
//                XSSFWorkbook xssfWb = (XSSFWorkbook) new XSSFWorkbook(fis);
                        File file = new File(filePath);
                        String fileNamee = file.getName();
                        boolean fileexist = file.exists();

                        //workBook = WorkbookFactory.create(file);
                        try (Workbook workBook = WorkbookFactory.create(file)) {
                            System.out.println("After::fileInputStream::" + new Date());
                            if (workBook.getSheetAt(sheetNo) instanceof XSSFSheet) {
                                sheet = (XSSFSheet) workBook.getSheetAt(sheetNo);
                            } else if (workBook.getSheetAt(sheetNo) instanceof HSSFSheet) {
                                sheet = (HSSFSheet) workBook.getSheetAt(sheetNo);
                            }

//                sheet = (XSSFSheet) xssfWb.getSheetAt(0);
//                        }
                            if (sheet != null) {
                                Row row = sheet.getRow(0);
                                if (row != null) {
                                    for (int j = 0; j < row.getLastCellNum(); j++) {
                                        //  System.out.println("Cell Num:::" + j + ":::Start Date And Time :::" + new Date());

                                        try {
                                            Cell cell = row.getCell(j);
                                            String header = "";
                                            if (cell != null) {
                                                switch (cell.getCellType()) {
                                                    case Cell.CELL_TYPE_STRING:
                                                        header = cell.getStringCellValue();
                                                        break;
                                                    case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
                                                        break;
                                                    case Cell.CELL_TYPE_NUMERIC:

                                                        if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                                            Date cellDate = cell.getDateCellValue();
                                                            if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12 && (cellDate.getDate()) == 31) {
                                                                header = (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
                                                            } else {
                                                                header = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
                                                            }

//                                                        String cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate());
//
                                                        } else {
                                                            header = NumberToTextConverter.toText(cell.getNumericCellValue());
                                                        }
                                                        break;
                                                    case Cell.CELL_TYPE_BLANK:
                                                        header = "";
                                                        break;
                                                }
                                                if (headerCount.containsKey(header.toLowerCase())) {
                                                    int count = headerCount.get(header.toLowerCase());
                                                    count++;
                                                    headerCount.put(header.toLowerCase(), count);
                                                    if (count == 2) {
                                                        header = header + "_1";
                                                    } else {
                                                        header = header + "_" + (count - 1);
                                                    }
                                                } else {
                                                    headerCount.put(header.toLowerCase(), 1);
                                                }
                                                headers.add(header);

                                            } else {
                                                headers.add("");
//                            testMap.put(stmt, "");
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            headers.add("");
                                            continue;
                                        }

                                    }// end of row cell loop
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace(); // Handle or log the exception appropriately
                        }

                    } else if ("xml".equalsIgnoreCase(fileExt)) {
                        headers = new ArrayList<>();
                        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                        DocumentBuilder builder = factory.newDocumentBuilder();
                        Document document = builder.parse(new File(filePath));
                        document.getDocumentElement().normalize();
                        Element root = document.getDocumentElement();
                        if (root.hasChildNodes() && root.getChildNodes().getLength() > 1) {
                            String evaluateTagName = "/" + root.getTagName();
                            NodeList rootList = root.getChildNodes();
                            if (!"#Text".equalsIgnoreCase(rootList.item(0).getNodeName())) {
                                evaluateTagName += "/" + rootList.item(0).getNodeName();
                            } else {
                                evaluateTagName += "/" + rootList.item(1).getNodeName();
                            }

                            System.out.println("evaluateTagName:::" + evaluateTagName);
                            XPath xpath = XPathFactory.newInstance().newXPath();
                            NodeList dataNodeList = (NodeList) xpath.evaluate(evaluateTagName,
                                    //            NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
                                    document,
                                    XPathConstants.NODESET);
                            if (dataNodeList != null && dataNodeList.getLength() != 0) {
                                int rowCount = dataNodeList.getLength();
                                Node node = dataNodeList.item(0);
                                if (node.getNodeType() == Node.ELEMENT_NODE) {
                                    NodeList childNodeList = node.getChildNodes();
                                    for (int i = 0; i < childNodeList.getLength(); i++) {// Columns

                                        Node childNode = childNodeList.item(i);
                                        if (childNode != null
                                                && !"#Text".equalsIgnoreCase(childNode.getNodeName())) {
                                            String header = childNode.getNodeName();
                                            if (headerCount.containsKey(header.toLowerCase())) {
                                                int count = headerCount.get(header.toLowerCase());
                                                count++;
                                                headerCount.put(header.toLowerCase(), count);
                                                if (count == 2) {
                                                    header = header + "_1";
                                                } else {
                                                    header = header + "_" + (count - 1);
                                                }
                                            } else {
                                                headerCount.put(header.toLowerCase(), 1);
                                            }
                                            headers.add(header);
                                            System.err.println(childNode.getNodeName() + "---> " + childNode.getTextContent());
                                        }

                                    }// end of columns loop

                                }

                            }

                        }
                    } else if ("pdf".equalsIgnoreCase(fileExt)) {
                        headers = new ArrayList();
                        String result = readPDFRestApi(request, filePath);
                        JSONObject apiPdfJsonData = (JSONObject) JSONValue.parse(result);
                        List<String> headerArray = (List<String>) apiPdfJsonData.get("columns");
                        List<List<String>> resultArray = (List<List<String>>) apiPdfJsonData.get("data");
                        if (resultArray != null && !resultArray.isEmpty()) {
                            LinkedHashMap rowObj = (LinkedHashMap) resultArray.get(0);
                            headers.addAll(rowObj.keySet());
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return headers;
    }

//    public List readPDFRestApi(HttpServletRequest request, String filePath) {
//        ArrayList responseBody = new ArrayList();
//        try {
//            if (filePath.contains("\\")) {
//                filePath = filePath.substring(filePath.lastIndexOf("\\") + 1);
//            }
//            if (filePath.contains("/")) {
//                filePath = filePath.substring(filePath.lastIndexOf("/") + 1);
//            }
//            HttpHeaders headers = new HttpHeaders();
//            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
//            MultiValueMap<String, String> inputMap = new LinkedMultiValueMap<>();
//            inputMap.add("tableName", "DAL_DM_SAVED_FILES");
//
//            inputMap.add("userName", (String) request.getSession(false).getAttribute("ssUsername"));
//            inputMap.add("orgnId", (String) request.getSession(false).getAttribute("ssOrgId"));
////            inputMap.add("FILE_NAME", "SPIRUploadSheet1642749028412.pdf");
//            inputMap.add("fileName", filePath);
//
//            HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
//                    headers);
//            RestTemplate template = new RestTemplate();
//            ResponseEntity<ArrayList> response = template.postForEntity("http://apihub.piloggroup.com:6654/pdfreader", entity,
//                    ArrayList.class);
//            responseBody = response.getBody();
//
//            System.out.println("");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return responseBody;
//
//    }
//    

    public String readPDFRestApi(HttpServletRequest request, String filePath) {
        String result = "";
        final int numberOfThreads = 1;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final List<Future<?>> futures = new ArrayList<>();
        try {
            String responseId = AuditIdGenerator.genRandom32Hex();
            executor.submit(() -> {
                try {
                    String threadName = Thread.currentThread().getName();
                    System.out.println("Hello " + threadName);
                    String userName = (String) request.getSession(false).getAttribute("ssUsername");
                    File file = new File(filePath);
                    FileSystemResource fileData = new FileSystemResource(file);
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.MULTIPART_FORM_DATA);
                    MultiValueMap<String, Object> inputMap = new LinkedMultiValueMap<>();
                    inputMap.add("fileName", fileData);
                    inputMap.add("responseId", responseId);
                    inputMap.add("accessName", accessName);
                    System.out.println("ACCESS NAME:::::::::::" + accessName);
                    HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(inputMap, headers);
                    RestTemplate template = new RestTemplate();
                    System.out.println("Request sent ::::: ");
                    long startTime = System.currentTimeMillis();
                    System.out.println("API called :: " + System.currentTimeMillis());
//                    ResponseEntity<String> responseObject = template.postForEntity("http://idxp.pilogcloud.com:6651/pdfreader", entity, String.class);
                    ResponseEntity<String> responseObject = template.postForEntity("http://apihub.pilogcloud.com:6671/ncc_value_extraction", entity, String.class);
                    String response = responseObject.getBody();
                    System.out.println("Response time :: " + (System.currentTimeMillis() - startTime) / 1000 + " Sec");
                } catch (Exception e) {
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
            result = getApiResponse(request, responseId, executor, 0);
        } catch (Exception e) {
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
        return result;
    }

    public String getApiResponse(HttpServletRequest request, String responseId, ExecutorService executor, int currentTimeInMilliSec) {
        String result = "";
        int maxWaitingTime = 300000;
        String apiResponseWaitTimeInMilliSec = (String) request.getParameter("apiResponseWaitTimeInMilliSec");
        if (apiResponseWaitTimeInMilliSec != null && !"".equalsIgnoreCase(apiResponseWaitTimeInMilliSec)
                && !"null".equalsIgnoreCase(apiResponseWaitTimeInMilliSec)) {
            maxWaitingTime = Integer.parseInt(apiResponseWaitTimeInMilliSec); // 5 Minutes
        }
        try {
            if (currentTimeInMilliSec >= maxWaitingTime) {
                executor.shutdown();
            }
//            String query = "SELECT RESPONSE_DATA FROM PYTHON_API_RESPONSE WHERE RESPONSE_ID='" + responseId + "'";
            String query = "SELECT RESPONSE_DATA FROM O_RECORD_WS_RESP_PYTHON_API WHERE RESPONSE_ID='" + responseId + "'";
            List list = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
            if (list != null && !list.isEmpty()) {
                result = new PilogUtilities().clobToString((Clob) list.get(0));
            } else {
                // ,each time the for loop runs
                currentTimeInMilliSec += 5000;
                Thread.sleep(5000);
                if (executor.isShutdown()) {
//                    Thread.sleep(10000);
                    query = "SELECT RESPONSE_DATA FROM O_RECORD_WS_RESP_PYTHON_API WHERE RESPONSE_ID='" + responseId + "'";
                    list = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
                    if (list != null && !list.isEmpty()) {
                        result = new PilogUtilities().clobToString((Clob) list.get(0));
                    }
                } else {
                    System.out.println("Response Waiting Time :::: " + currentTimeInMilliSec + "ms");
                    System.out.println("Thread waiting");
                    result = getApiResponse(request, responseId, executor, currentTimeInMilliSec);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<Object[]> getTreeDMTableColumnsOpt(Connection connection, HttpServletRequest request, JSONObject dbObj, String tableName) {
        List<Object[]> columnsList = new ArrayList<>();
        try {
            if (connection != null) {
                if (tableName != null && tableName.contains(",")) {
//                    tableName = tableName.replaceAll(",", "','");
                    List<String> fullTableNamesList = Arrays.asList(tableName.split(",")); // ravi process job issues
                    tableName = fullTableNamesList.stream().map(table -> table.split("\\.")[1]).collect(Collectors.joining(","));
                    // ---------------------------------------- process job issues end
                    tableName = tableName.replaceAll(",", "','");
                } else {
                    if (tableName != null
                            && !"".equalsIgnoreCase(tableName)
                            && !"null".equalsIgnoreCase(tableName)
                            && tableName.contains(".")) {
                        tableName = (tableName.split("[.]"))[1];
                    }
                }
                String initParamClassName = "com.pilog.mdm.DAO.V10DataMigrationAccessDAO";
                String initParamMethodName = "getTree" + dbObj.get("CONN_CUST_COL1") + "TableColumns";
                System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
                Class clazz = Class.forName(initParamClassName);
                Class<?>[] paramTypes = {Connection.class, HttpServletRequest.class, String.class, String.class};
                Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
                Object targetObj = new PilogUtilities().createObjectByClass(clazz);
                columnsList = (List<Object[]>) method.invoke(targetObj, connection, request, String.valueOf(dbObj.get("CONN_DB_NAME")), tableName);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsList;
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
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceColumnsList;
    }

    public IRepository retrieveRepository(JCO.Client connection) {
        try {
            theRepository = new JCO.Repository("saprep", connection);
        } catch (Exception ex) {
            System.out.println("failed to retrieve repository");
        }
        return theRepository;
    }

    // method for establishing SAP connection
    public JCO.Function getFunction(String name) {
        try {
            return theRepository.getFunctionTemplate(name.toUpperCase()).getFunction();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public List getSAPTableColumns(HttpServletRequest request, JCO.Client connection, String tableName) {
        if (theRepository == null) {
            retrieveRepository(connection);
        }

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
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sourceColumnsList;
    }

    public List fileHeaderValidations(List<String> headers) {
        try {
            String[] reservedKeyWords = {"ACCESS", "ELSE", "MODIFY", "START", "ADD", "EXCLUSIVE", "NOAUDIT", "SELECT", "ALL", "EXISTS", "NOCOMPRESS", "SESSION", "ALTER", "FILE", "NOT", "SET", "AND", "FLOAT", "NOTFOUND", "SHARE", "ANY", "FOR", "NOWAIT", "SIZE", "ARRAYLEN", "FROM", "NULL", "SMALLINT", "AS", "GRANT", "NUMBER", "SQLBUF", "ASC", "GROUP", "OF", "SUCCESSFUL", "AUDIT", "HAVING", "OFFLINE", "SYNONYM", "BETWEEN", "IDENTIFIED", "ON", "SYSDATE", "BY", "IMMEDIATE", "ONLINE", "TABLE", "CHAR", "IN", "OPTION", "THEN", "CHECK", "INCREMENT", "OR", "TO", "CLUSTER", "INDEX", "ORDER", "TRIGGER", "COLUMN", "INITIAL", "PCTFREE", "UID", "COMMENT", "INSERT", "PRIOR", "UNION", "COMPRESS", "INTEGER", "PRIVILEGES", "UNIQUE", "CONNECT", "INTERSECT", "PUBLIC", "UPDATE", "CREATE", "INTO", "RAW", "USER", "CURRENT", "IS", "RENAME", "VALIDATE", "DATE", "LEVEL", "RESOURCE", "VALUES", "DECIMAL", "LIKE", "REVOKE", "VARCHAR", "DEFAULT", "LOCK", "ROW", "VARCHAR2", "DELETE", "LONG", "ROWID", "VIEW", "DESC", "MAXEXTENTS", "ROWLABEL", "WHENEVER", "DISTINCT", "MINUS", "ROWNUM", "WHERE", "DROP", "MODE", "ROWS", "WITH"};
            List reservedKeyWordsList = Arrays.asList(reservedKeyWords);
            headers = headers.stream().map(col -> ((String) col).replaceAll("[^a-zA-Z0-9]", "_")).collect(Collectors.toList());

            List tempHeadersList = new ArrayList();
            Map duplicateHeadersList = new HashMap();

            for (int i = 0; i < headers.size(); i++) {
                String col = headers.get(i);
                if (tempHeadersList.contains(col)) {
                    duplicateHeadersList.put(col, (duplicateHeadersList.get(col) != null) ? ((int) duplicateHeadersList.get(col) + 1) : 1);
                    col = col + duplicateHeadersList.get(col);
                }
                if (reservedKeyWordsList.contains(col.toUpperCase())) {
                    duplicateHeadersList.put(col, (duplicateHeadersList.get(col) != null) ? ((int) duplicateHeadersList.get(col) + 1) : 1);
                    col = col + duplicateHeadersList.get(col);
                }
                tempHeadersList.add(col);
            }
            headers = tempHeadersList;
            headers = headers.stream().map(col -> {

                        if (((String) col).length() > 32) {
                            col = (String) col;
                            col = col.substring(col.length() - 31);
                        }
                        return col;
                    }
            ).collect(Collectors.toList());

            headers = headers.stream().map(col -> {
                        if (Character.isDigit(((String) col).charAt(0))) {
                            col = (String) col;
                            col = col.replace(String.valueOf(col.charAt(0)), "A" + col.charAt(0));
                        }
                        if (col.startsWith("_")) {
                            col = (String) col;
                            col = col.replace(String.valueOf(col.charAt(0)), "");
                        }
                        return col;
                    }
            ).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return headers;
    }

    public JSONObject getColumnsObjFromQuery(String selectQuery, Connection connection, JSONObject connObj) {
        JSONObject resultObj = new JSONObject();
        List dataTypesList = new ArrayList();
        List colsList = new ArrayList();
        PreparedStatement preparedStatement = null;
        try {

            String statingTable = "ZZ_" + AuditIdGenerator.genRandom32Hex();
            String query = "CREATE TABLE " + statingTable + " AS " + selectQuery;
            System.out.println("query :: " + query);
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.execute();
            List<Object[]> fromColumnsObjList = getTableColumnsOpt(connection, (JSONObject) connObj, statingTable);
            colsList = fromColumnsObjList.stream().map(e -> (String) ((Object[]) e)[2]).collect(Collectors.toList());
            dataTypesList = fromColumnsObjList.stream().map(e -> (String) ((Object[]) e)[8]).collect(Collectors.toList());
            dropStagingTable(statingTable, connection);
            resultObj.put("dataTypesList", dataTypesList);
            resultObj.put("columnsList", colsList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;

    }

    // method to convert type of value from Object to its actual type (like String, Int, etc)
    public Object convertIntoDBValue(String columnType,
                                     Object value,
                                     String columnName,
                                     String fromDateFormate
    ) {
        try {

            if (value != null && columnType != null
                    && !"".equalsIgnoreCase(columnType)
                    && !"null".equalsIgnoreCase(columnType)) {

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
                if ("DATE".equalsIgnoreCase(columnType)
                        || "TIMESTAMP".equalsIgnoreCase(columnType)
                        || "DATETIME".equalsIgnoreCase(columnType)) {
                    try {
                        JSONObject funObj = new JSONObject();
                        if (fromDateFormate != null
                                && !"".equalsIgnoreCase(fromDateFormate)
                                && !"null".equalsIgnoreCase(fromDateFormate)
                                && fromDateFormate.contains("{")) {
                            try {
                                JSONObject coluClauseObj = (JSONObject) JSONValue.parse(fromDateFormate);
                                String funobjstr = (String) coluClauseObj.get("funobjstr");
                                if (funobjstr != null
                                        && !"".equalsIgnoreCase(funobjstr)
                                        && !"null".equalsIgnoreCase(funobjstr)) {
                                    funObj = (JSONObject) JSONValue.parse(funobjstr);
                                    if (funObj != null && !funObj.isEmpty()) {
                                        if (funObj.get("funStr") != null
                                                && !"".equalsIgnoreCase(String.valueOf(funObj.get("funStr")))
                                                && !"null".equalsIgnoreCase(String.valueOf(funObj.get("funStr")))) {
                                            fromDateFormate = String.valueOf(funObj.get("funStr"));
                                        } else {
                                            fromDateFormate = (String) funObj.get("functionName");
                                        }

                                    }
                                }

                            } catch (Exception e) {
                            }
                        }
                        if (!(fromDateFormate != null
                                && !"".equalsIgnoreCase(fromDateFormate)
                                && !"null".equalsIgnoreCase(fromDateFormate))) {
                            fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                        }

                        //DATE_FORMAT('dd-MM-yyyy HH:mm:ss.sss','COLUMNS','undefined','undefined')
                        if (fromDateFormate.contains("DATE_FORMAT(")) {
                            if (funObj != null
                                    && !funObj.isEmpty()) {//FROM_DATE_FORMATE
                                if (funObj.get("FROM_DATE_FORMATE") != null
                                        && !"".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))
                                        && !"null".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))) {
                                    fromDateFormate = String.valueOf(funObj.get("FROM_DATE_FORMATE"));
                                } else {
                                    fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                                }

                            } else {
                                fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                            }
//                            fromDateFormate = fromDateFormate.substring(fromDateFormate.indexOf("'") + 1,
//                                    fromDateFormate.indexOf("'", fromDateFormate.indexOf("'") + 1));
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fromDateFormate);
                            Date localDate = simpleDateFormat.parse(String.valueOf(value));
                            if (localDate != null) {
                                java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                                value = sqlDate;
                            }
                        } else if (fromDateFormate.contains("SYSDATE")
                                || fromDateFormate.contains("CURDATE")) {//
                            if (funObj != null
                                    && !funObj.isEmpty()) {//FROM_DATE_FORMATE
                                if (funObj.get("FROM_DATE_FORMATE") != null
                                        && !"".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))
                                        && !"null".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))) {
                                    fromDateFormate = String.valueOf(funObj.get("FROM_DATE_FORMATE"));
                                } else {
                                    fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                                }

                            } else {
                                fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                            }
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fromDateFormate);
                            Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
                            if (localDate != null) {
                                java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                                value = sqlDate;
                            }
                        } else {
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
                            Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
                            if (localDate != null) {
                                java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                                value = sqlDate;
                            }
                        }

                    } catch (Exception e) {
                    }

                } else if ("NUMBER".equalsIgnoreCase(columnType)
                        || "NUMERIC".equalsIgnoreCase(columnType)
                        || "INTEGER".equalsIgnoreCase(columnType)
                        || "INT".equalsIgnoreCase(columnType)
                        || "BIGINT".equalsIgnoreCase(columnType)
                        || "TINYINT".equalsIgnoreCase(columnType)
                        || "SMALLINT".equalsIgnoreCase(columnType)
                        || "MEDIUMINT".equalsIgnoreCase(columnType)) {

                    BigInteger integerObj = null; //  // ravi etl integration
                    try {
                        integerObj = new BigInteger(String.valueOf(value));

                    } catch (Exception e) {
                        value = 0;
                    }
                    if (integerObj != null) {
                        value = integerObj.intValue();
                    }

//                    BigInteger integerObj = new BigInteger(String.valueOf(value));
//                    if (integerObj != null) {
//                        value = integerObj.intValue();
//                    }
                } else if ("FLOAT".equalsIgnoreCase(columnType)
                        || "DECIMAL".equalsIgnoreCase(columnType)
                        || "DOUBLE".equalsIgnoreCase(columnType)) {

                    BigInteger integerObj = null; // ravi etl integration
                    try {
                        integerObj = new BigInteger(String.valueOf(value));

                    } catch (Exception e) {
                        value = 0;
                    }
                    if (integerObj != null) {
                        value = integerObj.intValue();
                    }

//                    BigDecimal integerObj = new BigDecimal(String.valueOf(value));
//                    if (integerObj != null) {
//                        value = integerObj.doubleValue();
//                    }
                } else if ("VARCHAR".equalsIgnoreCase(columnType)
                        || "VARCHAR2".equalsIgnoreCase(columnType)) {

                    value = String.valueOf(value);
                } else if ("CLOB".equalsIgnoreCase(columnType)) {

                    value = new PilogUtilities().clobToString((Clob) value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    // method to convert type of value from Object to its actual type (like String, Int, etc)
    public Object convertIntoDBValue(
            Object value,
            String columnName,
            String fromDateFormate
    ) {
        try {
            if (value != null) {
                if (fromDateFormate != null
                        && !"".equalsIgnoreCase(fromDateFormate)
                        && !"null".equalsIgnoreCase(fromDateFormate)) {
                    JSONObject funObj = new JSONObject();
                    if (fromDateFormate.contains("{")) {
                        try {
                            JSONObject coluClauseObj = (JSONObject) JSONValue.parse(fromDateFormate);
                            String funobjstr = (String) coluClauseObj.get("funobjstr");
                            if (funobjstr != null
                                    && !"".equalsIgnoreCase(funobjstr)
                                    && !"null".equalsIgnoreCase(funobjstr)) {
                                funObj = (JSONObject) JSONValue.parse(funobjstr);
                                if (funObj != null && !funObj.isEmpty()) {
                                    if (funObj.get("funStr") != null
                                            && !"".equalsIgnoreCase(String.valueOf(funObj.get("funStr")))
                                            && !"null".equalsIgnoreCase(String.valueOf(funObj.get("funStr")))) {
                                        fromDateFormate = String.valueOf(funObj.get("funStr"));
                                    } else {
                                        fromDateFormate = (String) funObj.get("functionName");
                                    }

                                }
                            }

                        } catch (Exception e) {
                        }
                    }
                    //DATE_FORMAT('dd-MM-yyyy HH:mm:ss.sss','COLUMNS','undefined','undefined')
                    if (fromDateFormate.contains("DATE_FORMAT(")) {
//                        fromDateFormate = fromDateFormate.substring(fromDateFormate.indexOf("'") + 1,
//                                fromDateFormate.indexOf("'", fromDateFormate.indexOf("'") + 1));
                        if (funObj != null
                                && !funObj.isEmpty()) {//FROM_DATE_FORMATE
                            if (funObj.get("FROM_DATE_FORMATE") != null
                                    && !"".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))
                                    && !"null".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))) {
                                fromDateFormate = String.valueOf(funObj.get("FROM_DATE_FORMATE"));
                            } else {
                                fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                            }

                        } else {
                            fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                        }
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fromDateFormate);
                        Date localDate = simpleDateFormat.parse(String.valueOf(value));
                        if (localDate != null) {
                            java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                            value = sqlDate;
                        }
                    } else if (fromDateFormate.contains("SYSDATE")
                            || fromDateFormate.contains("CURDATE")
                            || fromDateFormate.contains("CURRENT_DATE")) {//
                        if (funObj != null
                                && !funObj.isEmpty()) {//FROM_DATE_FORMATE
                            if (funObj.get("FROM_DATE_FORMATE") != null
                                    && !"".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))
                                    && !"null".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))) {
                                fromDateFormate = String.valueOf(funObj.get("FROM_DATE_FORMATE"));
                            } else {
                                fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                            }

                        } else {
                            fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                        }
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fromDateFormate);
                        Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
                        if (localDate != null) {
                            java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                            value = sqlDate;
                        }
                    } else {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
                        Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
                        if (localDate != null) {
                            java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                            value = sqlDate;
                        }
                    }
                    value = value.toString();
                } else {
                    value = String.valueOf(value);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    public String getOracleDataTypeOfValue(String value, int length) {
        String dataType = "";
        try {
            if (value != null) {
                if (isNumeric(value)) {
                    dataType = "NUMBER";
                } else if (isValidDate(value)) {
                    dataType = "DATE";
                } else if (isBooleanValue(value)) {
                    dataType = "VARCHAR2(10)";
                } else if (isCharacter(value)) {
                    dataType = "VARCHAR2(4)";
                } else {
//                    dataType = "VARCHAR2(" + (length + 100) + ")";
                    dataType = "VARCHAR2(4000)";
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataType;
    }

    public boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            strNum = strNum.replace(",", "");
            double d = Double.parseDouble(strNum);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    public boolean isValidDate(String dateStr) {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//        sdf.setLenient(false);
        try {
            sdf.parse(dateStr);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    public boolean isBooleanValue(String str) {
        if (str == null) {
            return false;
        }
        if ("TRUE".equalsIgnoreCase(str) || "FALSE".equalsIgnoreCase(str)) {
            return true;
        } else {
            return false;
        }

    }

    public boolean isCharacter(String str) {
        if (str == null) {
            return false;
        }
        if (str.length() == 1) {
            return true;
        } else {
            return false;
        }

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

    public JSONArray getPrimaryKeyColumns(Connection connection, String tableName) {

        ResultSet resultSet = null;
        DatabaseMetaData metaData = null;
        JSONArray tablePKColumnArr = new JSONArray();

        try {

            metaData = connection.getMetaData();
            resultSet = metaData.getPrimaryKeys(null, null, tableName);
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                tablePKColumnArr.add(columnName);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
//                if (connection != null) {
//                    connection.close();
//                }
            } catch (Exception e) {
            }
        }
        return tablePKColumnArr;
    }

    public boolean checkTableExist(String tableName, Connection connection) {
        Boolean tableExist = false;
        try {
            String query = "SELECT * FROM " + tableName + " WHERE 1=2";

            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.executeQuery();
            tableExist = true;
        } catch (Exception e) {
            e.printStackTrace();
            tableExist = false;
        }
        return tableExist;
    }

    public int getTableCount(String tableName, Connection connection) {
        int count = 0;
        ResultSet resultset = null;
        try {
            String query = "SELECT COUNT(*) FROM " + tableName;

            PreparedStatement stmt = connection.prepareStatement(query);
            resultset = stmt.executeQuery();
            while (resultset.next()) {
                count = resultset.getInt(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            count = 0;
        }
        return count;
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
            query = " INSERT INTO " + tableName + " (" + columnsStr + ")"
                    + " VALUES (" + paramsStr + ")";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return query;
    }


    @Transactional
    public void processETLJobPreview(
            String sessionUserName,
            String orgnId,
            String jobId,
            String operatorId,
            String sourceCount
    ) {
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        try {

            String insertQuery = "INSERT INTO C_ETL_JOB_PREVIEW ("
                    + "ORGN_ID, "
                    + "JOB_ID, "
                    + "USER_NAME, "
                    + "OPERATOR_ID, "
                    + "OPERATOR_COUNT "
                    + ")"
                    + " VALUES (?,?,?,?,?)";
            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(insertQuery);
            logStmt.setObject(1, orgnId);//orgnId
            logStmt.setObject(2, jobId);//sessionUserName
            logStmt.setObject(3, sessionUserName);//orgnId
            logStmt.setObject(4, operatorId);//sourceTable
            logStmt.setObject(5, sourceCount);//sourceCount

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


    public int updateETLReconciliationClob(String orgnId, String jobId, String ssUserName, String sourceTable,
                                           String sourceCount, String targetTable, String proccessedRecordCount, String rejectedRecordCount,
                                           String comments,  String subJobId, JSONObject getUpdatedCols) {
        // TODO Auto-generated method stub
        Connection logConnection = null;
        PreparedStatement logStmt = null;
        int insertCount = 0;
        try {
            System.out.println("updateETLReconciliationClob");
            String insertQuery = "UPDATE C_ETL_RECONCILIATION SET " + "UPDATE_COUNT_INFO =? "// 1
                    + "WHERE SUB_JOB_ID =? "// 2
                    + "AND JOB_ID=? "// 3
                    + "AND USER_NAME=? ";// 4

            Class.forName(dataBaseDriver);
            logConnection = DriverManager.getConnection(dbURL, userName, password);
            logStmt = logConnection.prepareStatement(insertQuery);

            logStmt.setObject(1, getUpdatedCols.toJSONString());// updatedColsClob
            logStmt.setObject(2, subJobId);// comments
            logStmt.setObject(3, jobId);// orgnId
            logStmt.setObject(4, ssUserName);// orgnId
            insertCount = logStmt.executeUpdate();
//                    System.out.println("insert Log Count " + insertCount);
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
        return insertCount;
    }




}
