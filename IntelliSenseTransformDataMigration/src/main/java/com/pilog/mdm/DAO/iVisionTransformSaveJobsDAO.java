/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;



import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author Ravindar.P
 */
@Repository
public class iVisionTransformSaveJobsDAO {

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

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List getcDmJobsList(HttpServletRequest request, String jobId) {

        List<Object[]> cDmJobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT "
                    + " ORGN_ID, " //0
                    + " SEQUENCE_NO," //1
                    + " JOB_ID, " //2
                    + " JOB_DESCR, "//3
                    + " USER_NAME, "//4
                    + " TRFM_RULES_ID,"//5
                    + " TRFM_DEGN_DATA, "//6
                    + " JOBS_CUST_COL1 "//7
                    + " FROM C_DM_JOBS WHERE JOB_ID=:JOB_ID AND USER_NAME=:USER_NAME AND ORGN_ID =:ORGN_ID";
            map.put("JOB_ID", jobId);
            map.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            map.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            System.out.println(" query ::: " + query);
            cDmJobsList = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return cDmJobsList;
    }

    @Transactional
    public List<Object[]> fetchSavedJobs(HttpServletRequest request) {
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT JOB_ID, JOB_DESCR FROM C_DM_JOBS "
                    + "WHERE ORGN_ID=:ORGN_ID "
                    + "AND USER_NAME=:USER_NAME "
                    + "AND SEQUENCE_NO=:SEQUENCE_NO "
                    + "ORDER BY CREATE_DATE DESC";
            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("SEQUENCE_NO", 0);
            jobsList = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobsList;
    }

    @Transactional
    public List<Object[]> fetchSavedJobs(HttpServletRequest request, String jobType) {
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT JOB_ID, JOB_DESCR FROM C_DM_JOBS "
                    + "WHERE ORGN_ID=:ORGN_ID "
                    + "AND USER_NAME=:USER_NAME "
                    + "AND SEQUENCE_NO=:SEQUENCE_NO "
                    + "AND JOBS_CUST_COL2=:JOBS_CUST_COL2 "
                    + "ORDER BY CREATE_DATE DESC";
            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("SEQUENCE_NO", 0);
            map.put("JOBS_CUST_COL2", jobType);
            jobsList = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobsList;
    }

    @Transactional
    public List<Object[]> fetchPredefinedJobs(HttpServletRequest request) {
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT JOB_ID, JOB_DESCR FROM C_DM_JOBS "
                    + "WHERE ORGN_ID=:ORGN_ID "
                    + "AND USER_NAME=:USER_NAME "
                    + "AND SEQUENCE_NO=:SEQUENCE_NO "
                    + "AND JOBS_CUST_COL2=:JOBS_CUST_COL2 "
                    + "ORDER BY CREATE_DATE DESC";
            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("SEQUENCE_NO", 0);
            map.put("JOBS_CUST_COL2", "Y");
            jobsList = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobsList;
    }

    @Transactional
    public JSONObject getSavedJobData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        Map<String, Object> map = new HashMap<>();
        try {
            String jobId = request.getParameter("jobId");
            resultObj.put("flowChartData", getflowChartData(request, jobId));
            //resultObj.put("columnMappingData", getcolumnMappingData(request));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @Transactional
    public String getflowChartData(HttpServletRequest request, String jobId) {
        String flowChartDataStr = "";
        Map<String, Object> map = new HashMap<>();
        try {

            String query = "SELECT TRFM_DEGN_DATA FROM C_DM_JOBS WHERE JOB_ID=:JOB_ID ORDER BY SEQUENCE_NO DESC";
            map.put("JOB_ID", jobId);
            List<Object> list = access.sqlqueryWithParams(query, map);
            if (list != null && !list.isEmpty()) {
                if (list.get(0) instanceof Clob) {
                    flowChartDataStr = new PilogUtilities().clobToString((Clob) list.get(0));
                } else {
                    flowChartDataStr = String.valueOf(list.get(0));
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return flowChartDataStr;
    }

    @Transactional
    public JSONObject getJobTransformationRules(HttpServletRequest request) {
        JSONObject processJobDataObj = new JSONObject();
        String processJobData = "";
        String mappedObjectData = "";
        String jobId = request.getParameter("jobId");
        Map<String, Object> map = new HashMap<>();
        try {

            String query = "SELECT "
                    + "TRFM_DEGN_DATA, "
                    + "JOBS_CUST_COL1 " //0
                    + "FROM C_DM_JOBS  "
                    + "WHERE JOB_ID =:JOB_ID ORDER BY SEQUENCE_NO";
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
        }
        return processJobDataObj;

    }

    @Transactional
    public int cDmJobs(HttpServletRequest request, JSONObject trfmRulesObj, String mappedData, String trfmRulesId, String jobId, String jobName, int sequenceNo, String processJobData, JSONObject jobDetails) {
        int insertCount = -1;
        try {
            String insertJobQuery = "INSERT INTO C_DM_JOBS(ORGN_ID, "
                    + " SEQUENCE_NO, JOB_ID, JOB_DESCR, USER_NAME, TRFM_RULES_ID,"
                    + " TRFM_DEGN_DATA, JOBS_CUST_COL1, JOBS_CUST_COL2, CREATE_BY, EDIT_BY,JOBS_CUST_COL8,JOBS_CUST_COL5)"//FOLDER CODE
                    + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
            Map<Integer, Object> insertMap = new HashMap<>();
            insertMap.put(1, request.getSession(false).getAttribute("ssOrgId"));
            insertMap.put(2, sequenceNo);
            insertMap.put(3, jobId);
            insertMap.put(4, jobName);
            insertMap.put(5, request.getSession(false).getAttribute("ssUsername"));
            insertMap.put(6, trfmRulesId);
            insertMap.put(7, mappedData);
            insertMap.put(8, processJobData);  // ravi edit for copy job
            insertMap.put(9, jobDetails.get("jobType"));  // ravi edit data mpdeller job
            insertMap.put(10, request.getSession(false).getAttribute("ssUsername"));
            insertMap.put(11, request.getSession(false).getAttribute("ssUsername"));
            String folderId = (String) jobDetails.get("folderId");
            String folderName = (String) jobDetails.get("folderName");
            insertMap.put(12, folderName);//folder code
            insertMap.put(13, folderId);//folder code

            insertCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(insertJobQuery, insertMap);
            System.out.println("insertCount:::" + insertCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    @Transactional
    public int saveFolderName(HttpServletRequest request, String jobType, String treeParentId, String folderName) {
        int insertCount = -1;
        String jobId = AuditIdGenerator.genRandom32Hex();
        String childFolderId = AuditIdGenerator.genRandom32Hex();
        try {
            String folderObjStr = request.getParameter("folderObj");
            JSONObject folderObj = (JSONObject) JSONValue.parse(folderObjStr);
            String parentFolder = (String) folderObj.get("parentFolder");
            String parentFolderId = (String) folderObj.get("parentFolderId");
            Long level = (Long) folderObj.get("treeLevel");

            String insertJobQuery = "INSERT INTO C_DM_JOBS("
                    + "ORGN_ID, " //1
                    + " SEQUENCE_NO,"//2
                    + " JOB_ID, "//3
                    + "JOB_DESCR,"//4
                    + " USER_NAME, "//5
                    + "   CREATE_BY, "//6
                    + "EDIT_BY,"//7
                    + "JOBS_CUST_COL8,"//8
                    + "JOBS_CUST_COL9,"//9
                    + "TRFM_DEGN_DATA,"//10
                    + "JOBS_CUST_COL2,"//11
                    + "JOBS_CUST_COL5,"//12
                    + "JOBS_CUST_COL6)"//13
                    + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
            Map<Integer, Object> insertMap = new HashMap<>();
            insertMap.put(1, request.getSession(false).getAttribute("ssOrgId"));
            insertMap.put(2, 0);
            insertMap.put(3, jobId);
            insertMap.put(4, treeParentId);
            insertMap.put(5, request.getSession(false).getAttribute("ssUsername"));
            insertMap.put(6, request.getSession(false).getAttribute("ssUsername"));
            insertMap.put(7, request.getSession(false).getAttribute("ssUsername"));
            insertMap.put(8, parentFolder);
            insertMap.put(9, folderName);
            insertMap.put(10, request.getParameter("jobDetails"));
            insertMap.put(11, jobType);
            insertMap.put(12, parentFolderId);
            insertMap.put(13, childFolderId);

            insertCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(insertJobQuery, insertMap);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    @Transactional
    public List<Object[]> fetchfoldersList(HttpServletRequest request, String jobType, String folderName, String folderId) {
        List<Object[]> foldersList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {

            String query = "SELECT JOBS_CUST_COL6,JOBS_CUST_COL9 FROM C_DM_JOBS WHERE "
                    + " ORGN_ID=:ORGN_ID AND "
                    + " USER_NAME=:USER_NAME AND "
                    + "JOBS_CUST_COL2=:JOBS_CUST_COL2 AND "
                    + "JOBS_CUST_COL8=:JOBS_CUST_COL8 AND "
                    + "JOBS_CUST_COL5=:JOBS_CUST_COL5 AND "
                    + "JOBS_CUST_COL9 IS NOT NULL";

            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("JOBS_CUST_COL8", folderName);
            map.put("JOBS_CUST_COL5", folderId);
            map.put("JOBS_CUST_COL2", jobType.toUpperCase());
            foldersList = access.sqlqueryWithParams(query, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return foldersList;
    }

    @Transactional
    public List<Object[]> fetchJobsListInFolder(HttpServletRequest request, String jobType, String folderName, String folderId) {
        List<Object[]> foldersList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {

            String query = "SELECT JOB_ID,JOB_DESCR FROM C_DM_JOBS WHERE "
                    + " ORGN_ID=:ORGN_ID AND "
                    + " USER_NAME=:USER_NAME AND "
                    + "JOBS_CUST_COL2=:JOBS_CUST_COL2 AND "
                    + "JOBS_CUST_COL8=:JOBS_CUST_COL8 AND "
                    //                    + "JOBS_CUST_COL5=:JOBS_CUST_COL5 AND "
                    + "JOBS_CUST_COL9 IS NULL";

            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("JOBS_CUST_COL8", folderName);
//            map.put("JOBS_CUST_COL5", folderId);
            map.put("JOBS_CUST_COL2", jobType.toUpperCase());
            foldersList = access.sqlqueryWithParams(query, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return foldersList;
    }

    //    @Transactional
//    public List<Object[]> fetchSavedGroupBy(HttpServletRequest request, String jobType) {
//        List<Object[]> jobsList = new ArrayList<>();
//        Map<String, Object> map = new HashMap<>();
//        try {
//            String query = "SELECT distinct JOBS_CUST_COL8 FROM C_DM_JOBS "
//                    + "WHERE ORGN_ID=:ORGN_ID "
//                    + "AND USER_NAME=:USER_NAME "
//                    + "AND JOBS_CUST_COL2=:JOBS_CUST_COL2  "
//                    + " "
//                    + "";
//            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
//            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
//            map.put("JOBS_CUST_COL2", jobType);
//            jobsList = access.sqlqueryWithParams(query, map);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return jobsList;
//    }
    @Transactional
    public List<Object[]> fetchSavedJobs(HttpServletRequest request, String jobType, String parentIdStr) {
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT JOB_ID,JOB_DESCR FROM C_DM_JOBS "
                    + "WHERE ORGN_ID=:ORGN_ID "
                    + "AND USER_NAME=:USER_NAME  AND JOBS_CUST_COL2=:JOBS_CUST_COL2 ";
            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("JOBS_CUST_COL2", jobType);
            if (parentIdStr != null && !"".equalsIgnoreCase(parentIdStr) && !"null".equalsIgnoreCase(parentIdStr)) {
                query += "AND JOBS_CUST_COL8=:JOBS_CUST_COL8 "
                        + "ORDER BY CREATE_DATE DESC,"
                        + "JOBS_CUST_COL8 ASC";
                map.put("JOBS_CUST_COL8", parentIdStr);
            } else {
                query += "AND JOBS_CUST_COL8 IS NULL "
                        + "ORDER BY CREATE_DATE DESC,"
                        + "JOBS_CUST_COL8 ASC";
            }
            jobsList = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobsList;
    }

    @Transactional
    public List<Object[]> getSubFoldersList(HttpServletRequest request, String folderName, String folderId) {
        List<Object[]> subFoldersList = new ArrayList();
        try {
            String query = "SELECT JOBS_CUST_COL6, JOBS_CUST_COL9 FROM C_DM_JOBS"
                    + " WHERE USER_NAME=:USER_NAME AND "
                    + "ORGN_ID=:ORGN_ID AND "
                    + "JOBS_CUST_COL8=:JOBS_CUST_COL8 AND "
                    + "JOBS_CUST_COL5=:JOBS_CUST_COL5";
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
            queryMap.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            queryMap.put("JOBS_CUST_COL8", folderName);
            queryMap.put("JOBS_CUST_COL5", folderId);
            System.out.println("sub folderslist query:::" + query);
            subFoldersList = access.sqlqueryWithParams(query, queryMap);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return subFoldersList;
    }

    @Transactional
    public int deleteETLFolder(HttpServletRequest request, String folderName, String folderId) {
        int deleteCount = -1;
        try {
            String deleteQuery = "DELETE FROM C_DM_JOBS "
                    + " WHERE USER_NAME=:USER_NAME AND "
                    + "ORGN_ID=:ORGN_ID AND "
                    + "JOBS_CUST_COL9=:JOBS_CUST_COL9 AND "
                    + "JOBS_CUST_COL6=:JOBS_CUST_COL6";
            Map<String, Object> deleteMap = new HashMap<>();
            deleteMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
            deleteMap.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            deleteMap.put("JOBS_CUST_COL9", folderName);
            deleteMap.put("JOBS_CUST_COL6", folderId);
            deleteCount = access.executeUpdateSQLNoAudit(deleteQuery, deleteMap);
            System.out.println("deleteCount:::" + deleteCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return deleteCount;
    }

    @Transactional
    public List<Object[]> getChildTreeData(HttpServletRequest request, String jobType, String folderId, String folderName) {
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT JOBS_CUST_COL6,JOBS_CUST_COL9 "
                    + "FROM C_DM_JOBS WHERE "
                    + "USER_NAME=:USER_NAME AND "
                    + "ORGN_ID=:ORGN_ID AND "
                    + "JOBS_CUST_COL2=:JOBS_CUST_COL2 AND "
                    + "JOBS_CUST_COL8=:JOBS_CUST_COL8 AND "
                    + "JOBS_CUST_COL5=:JOBS_CUST_COL5  "
                    + "AND JOBS_CUST_COL9 IS NOT NULL";
            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("JOBS_CUST_COL2", jobType);
            map.put("JOBS_CUST_COL8", folderName);
            map.put("JOBS_CUST_COL5", folderId);
            jobsList = access.sqlqueryWithParams(query, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobsList;
    }

    @Transactional
    public List<Object[]> fetchSavedGroupBy(HttpServletRequest request, String jobType) {
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String query = "SELECT DISTINCT JOBS_CUST_COL5,JOBS_CUST_COL8 "
                    + "FROM C_DM_JOBS "
                    + "WHERE ORGN_ID=:ORGN_ID AND "
                    + "USER_NAME=:USER_NAME "
                    + "AND JOBS_CUST_COL2=:JOBS_CUST_COL2 "
                    + " AND JOBS_CUST_COL9 IS NOT NULL";

            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("JOBS_CUST_COL2", jobType);
            jobsList = access.sqlqueryWithParams(query, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobsList;
    }

    @Transactional
    public void deleteFolderJobs(HttpServletRequest request, String folderName, String folderId) {
        int deleteCount = -1;
        try {
            String deleteQuery = "DELETE FROM C_DM_JOBS "
                    + " WHERE USER_NAME=:USER_NAME AND "
                    + "ORGN_ID=:ORGN_ID AND "
                    + "JOB_ID=:JOB_ID ";
            Map<String, Object> deleteMap = new HashMap<>();
            deleteMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
            deleteMap.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            deleteMap.put("JOB_ID", folderId);
            deleteCount = access.executeUpdateSQLNoAudit(deleteQuery, deleteMap);
            System.out.println("deleteCount:::" + deleteCount);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Transactional
    public List<Object[]> getSubFolderJobs(HttpServletRequest request, String folderName, String folderId) {
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        String jobType = request.getParameter("jobType");
        try {
            String query = "SELECT  JOB_ID,JOB_DESCR FROM C_DM_JOBS "
                    + "WHERE ORGN_ID=:ORGN_ID "
                    + "AND USER_NAME=:USER_NAME "
                    + "AND JOBS_CUST_COL2=:JOBS_CUST_COL2 "
                    + "AND JOBS_CUST_COL5=:JOBS_CUST_COL5 ";
            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("JOBS_CUST_COL5", folderId);
            map.put("JOBS_CUST_COL2", jobType);
            jobsList = access.sqlqueryWithParams(query, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobsList;
    }

    @Transactional
    public int deleteCDmJobs(HttpServletRequest request, String jobId) {
        int deleteCount = 0;
        try {
            Map<Integer, Object> deleteMap = new HashMap<>();
            String currentUser = (String) request.getSession(false).getAttribute("ssUsername");
            String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            String deleteCDmJobs = "DELETE FROM C_DM_JOBS WHERE JOB_ID IN ('" + jobId + "') AND ORGN_ID = ? AND USER_NAME = ?"; // ravi save new job
            deleteMap.put(1, orgnId);
            deleteMap.put(2, currentUser);
            deleteCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(deleteCDmJobs, deleteMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return deleteCount;

    }

    @Transactional
    public String renameJob(HttpServletRequest request, String jobId, String jobName) {
        String message = "";
        try {
            Map<String, Object> updateMap = new HashMap<>();
            String updateQuery = "UPDATE C_DM_JOBS SET JOB_DESCR=:JOB_DESCR WHERE JOB_ID=:JOB_ID";
            updateMap.put("JOB_DESCR", jobName);
            updateMap.put("JOB_ID", jobId);
            int updateCount = access.executeUpdateSQL(updateQuery, updateMap);
            if (updateCount > 0) {
                message = "Job Renamed Sucessfully";
            } else {
                message = "Job Rename Failed";
            }
        } catch (Exception e) {
            message = "Job Rename Failed";
            e.printStackTrace();
        }
        return message;

    }


    public JSONObject getSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String jobType = request.getParameter("jobType");
            String folderId = request.getParameter("folderId");
            String query = "";
            String processFlag = request.getParameter("processFlag");

            query = "SELECT  "
                    + "JOB_ID,"
                    + "JOB_DESCR,"
                    + "JOBS_CUST_COL5,"
                    + "JOBS_CUST_COL6,"
                    + "JOBS_CUST_COL8,"
                    + "JOBS_CUST_COL9, "
                    + "CREATE_DATE "
                    + "FROM "
                    + "C_DM_JOBS WHERE "
                    + "USER_NAME=:USER_NAME AND "
                    + "ORGN_ID=:ORGN_ID AND "
                    + "JOBS_CUST_COL2=:JOBS_CUST_COL2 AND "
                    // + "JOBS_CUST_COL8=:JOBS_CUST_COL8 AND "
                    + "JOBS_CUST_COL5=:JOBS_CUST_COL5 ORDER BY JOBS_CUST_COL9 ";
            // + "AND JOBS_CUST_COL9 IS NOT NULL";
            if(processFlag.equals("date")){
                query += ", CREATE_DATE DESC";
            }
            else if(processFlag.equals("name")){
                query += ", JOB_DESCR";
            }else if(processFlag.equals("type")){
                query += "DESC";
            }
            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("JOBS_CUST_COL2", jobType);
            //map.put("JOBS_CUST_COL8", folderName);
            map.put("JOBS_CUST_COL5", folderId);
            jobsList = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        resultObj.put("jobsList",jobsList);
        return resultObj;
    }


    @Transactional
    public JSONObject filterSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            String jobType = request.getParameter("jobType");
            String filterValue = request.getParameter("filterValue");

            String query = "SELECT  "
                    + "JOB_ID,"
                    + "JOB_DESCR,"
                    + "JOBS_CUST_COL5,"
                    + "JOBS_CUST_COL6,"
                    + "JOBS_CUST_COL8,"
                    + "JOBS_CUST_COL9, "
                    + "CREATE_DATE "
                    + "FROM "
                    + "C_DM_JOBS WHERE "
                    + "USER_NAME=:USER_NAME AND "
                    + "ORGN_ID=:ORGN_ID AND "
                    + "JOBS_CUST_COL2=:JOBS_CUST_COL2 AND "
                    // + "JOBS_CUST_COL8=:JOBS_CUST_COL8 AND "
                    + "UPPER(JOB_DESCR) LIKE '%"+filterValue.toUpperCase()+"%' ORDER BY JOB_DESCR";
            // + "AND JOBS_CUST_COL9 IS NOT NULL";
            map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            map.put("JOBS_CUST_COL2", jobType);

            jobsList = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }
        resultObj.put("jobsList",jobsList);
        return resultObj;
    }


    @Transactional
    public JSONObject renameETLFolder(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        String result = "";
        try {
            String jobType = request.getParameter("jobType");
            String folderId = request.getParameter("folderId");
            String newFolderName = request.getParameter("newFolderName");
            if (newFolderName!=null && !"".equalsIgnoreCase(newFolderName)){
                Map<String, Object> map = new HashMap<>();
                String updateQuery = "UPDATE C_DM_JOBS SET JOBS_CUST_COL8=:NEW_FOLDER_NAME "
                        + " WHERE JOBS_CUST_COL5=:FOLDER_ID"
                        + " AND ORGN_ID=:ORGN_ID"
                        + " AND USER_NAME=:USER_NAME"
                        + " AND JOBS_CUST_COL2=:JOB_TYPE";

                map.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
                map.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
                map.put("FOLDER_ID", folderId);
                map.put("NEW_FOLDER_NAME", newFolderName);
                map.put("JOB_TYPE", jobType);

                int updateCount = access.executeUpdateSQLNoAudit(updateQuery, map);

                Map<String, Object> map1 = new HashMap<>();
                String updateQuery1 = "UPDATE C_DM_JOBS SET JOBS_CUST_COL9=:NEW_FOLDER_NAME "
                        + " WHERE JOBS_CUST_COL6=:FOLDER_ID"
                        + " AND ORGN_ID=:ORGN_ID"
                        + " AND USER_NAME=:USER_NAME"
                        + " AND JOBS_CUST_COL2=:JOB_TYPE";

                map1.put("NEW_FOLDER_NAME", newFolderName);
                map1.put("FOLDER_ID", folderId);
                map1.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
                map1.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));

                map1.put("JOB_TYPE", jobType);

                int updateCount1 = access.executeUpdateSQLNoAudit(updateQuery1, map1);
                result = "Folder Renamed";
            } else {
                result = "Failed to Rename : ";
            }

        } catch (Exception e) {
            e.printStackTrace();
            result = "Failed to Rename : "+e.getMessage();
        }
        resultObj.put("result",result);
        return resultObj;
    }

    @Transactional
    public JSONObject moveSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        String folderName = request.getParameter("folderName");
        String folderId = request.getParameter("folderId");
        String jobId = request.getParameter("jobId");
        String message = "";
        try {
            Map<String, Object> updateMap = new HashMap<>();
            String updateQuery = "UPDATE C_DM_JOBS SET JOBS_CUST_COL8=:JOBS_CUST_COL8, JOBS_CUST_COL5=:JOBS_CUST_COL5 WHERE JOB_ID=:JOB_ID";
            updateMap.put("JOBS_CUST_COL8", folderName);
            updateMap.put("JOBS_CUST_COL5", folderId);
            updateMap.put("JOB_ID", jobId);
            int updateCount = access.executeUpdateSQL(updateQuery, updateMap);
            if (updateCount > 0) {
                message = "Job Moved Sucessfully";
            } else {
                message = "Job Move Failed";
            }
        } catch (Exception e) {
            message = "Job Move Failed";
            e.printStackTrace();
        }
        resultObj.put("message",message);
        return resultObj;

    }

    @Transactional
    public String shareSavedJob(HttpServletRequest request, String jobId, String username) {
        String message = "";
        try {
            Map<Integer, Object> params = new HashMap<>();
            String currentUser = (String) request.getSession(false).getAttribute("ssUsername");
            String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            String userCheckQuery = "SELECT USER_NAME FROM S_PERS_DETAIL WHERE PERS_ID IN (SELECT PERS_ID FROM S_PERSONNEL WHERE ORGN_ID = ?) AND USER_NAME = ?";
            params.put(1, orgnId);
            params.put(2, username);
            int userCheckCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(userCheckQuery, params);
            if(userCheckCount <= 0) {
                return "User Does not Exist";
            }
            String checkQuery = "SELECT 1 FROM C_DM_JOBS WHERE JOB_ID = ? AND USER_NAME = ?";
            params.clear();
            params.put(1, jobId);
            params.put(2, username);
            int checkCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(checkQuery, params);
            System.out.println("insertCount:::" + checkCount);
            if(checkCount > 0) {
                String updateQuery = "UPDATE C_DM_JOBS SET "
                        + " ORGN_ID = ORGN_ID, " // Assuming ORGN_ID remains the same
                        + " SEQUENCE_NO = SEQUENCE_NO, " // Assuming SEQUENCE_NO remains the same
                        + " JOB_DESCR = JOB_DESCR, " // Assuming JOB_DESCR remains the same
                        + " TRFM_RULES_ID = TRFM_RULES_ID, " // Assuming TRFM_RULES_ID remains the same
                        + " TRFM_DEGN_DATA = TRFM_DEGN_DATA, " // Assuming TRFM_DEGN_DATA remains the same
                        + " JOBS_CUST_COL1 = JOBS_CUST_COL1, " // Assuming JOBS_CUST_COL1 remains the same
                        + " JOBS_CUST_COL2 = JOBS_CUST_COL2, " // Assuming JOBS_CUST_COL2 remains the same
                        + " JOBS_CUST_COL3 = JOBS_CUST_COL3, " // Assuming JOBS_CUST_COL3 remains the same
                        + " JOBS_CUST_COL4 = JOBS_CUST_COL4, " // Assuming JOBS_CUST_COL4 remains the same
                        + " JOBS_CUST_COL5 = JOBS_CUST_COL5, " // Assuming JOBS_CUST_COL5 remains the same
                        + " JOBS_CUST_COL6 = JOBS_CUST_COL6, " // Assuming JOBS_CUST_COL6 remains the same
                        + " JOBS_CUST_COL7 = JOBS_CUST_COL7, " // Assuming JOBS_CUST_COL7 remains the same
                        + " JOBS_CUST_COL8 = JOBS_CUST_COL8, " // Assuming JOBS_CUST_COL8 remains the same
                        + " JOBS_CUST_COL9 = JOBS_CUST_COL9, " // Assuming JOBS_CUST_COL9 remains the same
                        + " JOBS_CUST_COL10 = JOBS_CUST_COL10, " // Assuming JOBS_CUST_COL10 remains the same
                        + " EDIT_BY = EDIT_BY, " // Set the new EDIT_BY value
                        + " EDIT_DATE = SYSDATE " // Set the new EDIT_DATE value
                        + "WHERE JOB_ID = ? AND USER_NAME = ?";

                Map<Integer, Object> updateParams = new HashMap<>();
                updateParams.put(1, jobId); // JOB_ID
                updateParams.put(2, username); // USER_NAME

                int updateCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(updateQuery, updateParams);
                System.out.println("updateCount:::" + updateCount);
                if (updateCount > 0) {
                    message = "Job already present, updated successfully";
                } else {
                    message = "Job already present, update failed";
                }
            } else {
                String insertQuery = "INSERT INTO C_DM_JOBS ("
                        + " ORGN_ID,"
                        + " SEQUENCE_NO,"
                        + " JOB_ID,"
                        + " JOB_DESCR,"
                        + " USER_NAME,"
                        + " TRFM_RULES_ID,"
                        + " TRFM_DEGN_DATA,"
                        + " JOBS_CUST_COL1,"
                        + " JOBS_CUST_COL2,"
                        + " JOBS_CUST_COL3,"
                        + " JOBS_CUST_COL4,"
                        + " JOBS_CUST_COL5,"
                        + " JOBS_CUST_COL6,"
                        + " JOBS_CUST_COL7,"
                        + " JOBS_CUST_COL8,"
                        + " JOBS_CUST_COL9,"
                        + " JOBS_CUST_COL10,"
                        + " CREATE_BY,"
                        + " EDIT_BY"
                        + ")"
                        + " SELECT"
                        + " ORGN_ID,"
                        + " SEQUENCE_NO,"
                        + " JOB_ID,"
                        + " JOB_DESCR,"
                        + " ?,"
                        + " TRFM_RULES_ID,"
                        + " TRFM_DEGN_DATA,"
                        + " JOBS_CUST_COL1,"
                        + " JOBS_CUST_COL2,"
                        + " JOBS_CUST_COL3,"
                        + " JOBS_CUST_COL4,"
                        + " JOBS_CUST_COL5,"
                        + " JOBS_CUST_COL6,"
                        + " JOBS_CUST_COL7,"
                        + " JOBS_CUST_COL8,"
                        + " JOBS_CUST_COL9,"
                        + " JOBS_CUST_COL10,"
                        + " ?,"
                        + " ?"
                        + " FROM C_DM_JOBS"
                        + " WHERE"
                        + " JOB_ID = ?"
                        + " AND USER_NAME = ?"
                        + " AND NOT EXISTS ("
                        + " SELECT 1 FROM C_DM_JOBS"
                        + " WHERE JOB_ID = ? AND USER_NAME = ?)";
                params.clear();
                params.put(1, username); // USER_NAME
                params.put(2, username); // CREATE_BY
                params.put(3, username); // EDIT_BY
                params.put(4, jobId); // JOB_ID for the main query
                params.put(5, currentUser);
                params.put(6, jobId); // JOB_ID for the NOT EXISTS subquery
                params.put(7, username); // USER_NAME for the NOT EXISTS subquery
                int insertCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(insertQuery, params);
                System.out.println("insertCount:::" + insertCount);
                if (insertCount > 0) {
                    message = "Job Shared Sucessfully";
                } else {
                    message = "Job Shared Failed";
                }
            }


        } catch (Exception e) {
            message = "Job Shared Failed";
            e.printStackTrace();
        }
        return message;

    }

}
