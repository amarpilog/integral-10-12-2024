/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;


import com.pilog.mdm.access.DataAccess;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author Uttej.K
 */
@Repository
public class iVisionTransformationGroupJobsDAO {

    @Autowired
    private DataAccess access;

    @Transactional
    public int updateGroupJobs(HttpServletRequest request, String jobId, String groupJobData, int sequenceNo) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE C_DM_JOBS SET  TRFM_DEGN_DATA=:TRFM_DEGN_DATA,SEQUENCE_NO=:SEQUENCE_NO"
                    + " WHERE JOB_ID = :JOB_ID AND ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME AND CREATE_BY=:CREATE_BY ";
            System.out.println("query::" + updateQuery);
            Map updateResultMap = new HashMap();
            updateResultMap.put("TRFM_DEGN_DATA", groupJobData);
            updateResultMap.put("JOB_ID", jobId);
            updateResultMap.put("SEQUENCE_NO", sequenceNo);
            updateResultMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
            updateResultMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
            updateResultMap.put("CREATE_BY", request.getSession(false).getAttribute("ssUsername"));
            updateCount = access.executeUpdateSQL(updateQuery, updateResultMap);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return updateCount;

    }

    @Transactional
    public List<Object[]> fetchJobs(HttpServletRequest request, String jobType) {
        List<Object[]> jobsList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {

            String query = "SELECT JOB_ID,JOB_DESCR FROM C_DM_JOBS "
                    + "WHERE ORGN_ID=:ORGN_ID "
                    + "AND USER_NAME=:USER_NAME "
                    + "AND (JOBS_CUST_COL10 <> 'GROUP'  OR JOBS_CUST_COL10 IS NULL) "//group job
                    + "AND (JOB_DESCR <> 'PARENT_ID') "//group job
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
    public int saveGroupJobs(HttpServletRequest request, String jobId, String jobName, JSONObject jobDetails, String groupJobData, int sequenceNo) {
        int insertCount = -1;
        try {
            String folderId = request.getParameter("folderId");
            String folderName = request.getParameter("folderName");
            String insertJobQuery = "INSERT INTO C_DM_JOBS(ORGN_ID, "
                    + " SEQUENCE_NO, JOB_ID, JOB_DESCR, USER_NAME, "
            + "  JOBS_CUST_COL2, CREATE_BY, EDIT_BY,JOBS_CUST_COL10,TRFM_DEGN_DATA,JOBS_CUST_COL8,JOBS_CUST_COL5)"
                    + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
            Map<Integer, Object> insertMap = new HashMap<>();
            insertMap.put(1, request.getSession(false).getAttribute("ssOrgId"));
            insertMap.put(2, sequenceNo);
            insertMap.put(3, jobId);
            insertMap.put(4, jobName);
            insertMap.put(5, request.getSession(false).getAttribute("ssUsername"));
            insertMap.put(6, jobDetails.get("jobType"));
            insertMap.put(7, request.getSession(false).getAttribute("ssUsername"));
            insertMap.put(8, request.getSession(false).getAttribute("ssUsername"));
            insertMap.put(9, jobDetails.get("jobDetail"));
            insertMap.put(10, groupJobData);
            insertMap.put(11, folderName);
            insertMap.put(12, folderId);
            insertCount = access.executeNativeUpdateSQLWithSimpleParamsNoAudit(insertJobQuery, insertMap);
            System.out.println("insertCount:::" + insertCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;

    }
}
