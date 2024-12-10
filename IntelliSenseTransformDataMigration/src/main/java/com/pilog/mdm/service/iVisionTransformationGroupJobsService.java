/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;


import com.pilog.mdm.DAO.iVisionTransformSaveJobsDAO;
import com.pilog.mdm.DAO.iVisionTransformationGroupJobsDAO;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author Uttej.K
 */
@Service
public class iVisionTransformationGroupJobsService {

    @Autowired
    private iVisionTransformationGroupJobsDAO groupJobsDAO;

    @Autowired
    private iVisionTransformSaveJobsDAO saveJobsDAO;

    public JSONObject fetchJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            JSONArray jobScheduledArray = new JSONArray();
            JSONObject tableObj = new JSONObject();
            tableObj.put("id", "SAVED_JOB");
            tableObj.put("text", "Saved Jobs");
            tableObj.put("value", "SAVED_JOBS");
            jobScheduledArray.add(tableObj);
            List<Object[]> jobsList = groupJobsDAO.fetchJobs(request, "ETL");
            if (jobsList != null && !jobsList.isEmpty()) {
                for (Object[] scheduledJobsArray : jobsList) {
                    if (scheduledJobsArray != null && scheduledJobsArray.length != 0) {
                        tableObj = new JSONObject();
                        tableObj.put("id", scheduledJobsArray[0]);
                        tableObj.put("text", scheduledJobsArray[1]);
                        tableObj.put("value", scheduledJobsArray[1]);
                        tableObj.put("parentid", "SAVED_JOB");

                        jobScheduledArray.add(tableObj);
                    }
                }

            }
            resultObj.put("scheduledJobsArray", jobScheduledArray);
        } catch (Exception e) {
        }
        return resultObj;
    }

    public JSONObject saveGroupjob(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        String message = "";
        String jobIdStr = "";
        try {
            JSONObject labelObject = new PilogUtilities().getMultilingualObject(request);
            String jobName = request.getParameter("jobName");
            String jobId = AuditIdGenerator.genRandom32Hex();
            String jobDetailsStr = request.getParameter("jobDetails");
            String checkedItemsStr = request.getParameter("checkedItemsObj");

            JSONObject jobDetails = (JSONObject) JSONValue.parse(jobDetailsStr);
            JSONObject checkedItemsObj = (JSONObject) JSONValue.parse(checkedItemsStr);

            jobIdStr = saveGroupJobs(request, jobId, jobName, jobDetails, checkedItemsStr);
            if (!"".equalsIgnoreCase(jobIdStr)) {
                message = new PilogUtilities().convertIntoMultilingualValue(labelObject, "Job updated succesfully with") + " " + jobName;
            } else {
                message = "Error saving Job";
                message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObject, message);
            }

            resultObj.put("message", message);
            resultObj.put("jobId", jobIdStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject updateGroupjob(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        String message = "";
        String jobIdStr = "";
        try {
            JSONObject labelObject = new PilogUtilities().getMultilingualObject(request);
            String jobId = request.getParameter("jobId");
            String checkedItemsStr = request.getParameter("checkedItemsObj");
            jobIdStr = updateGroupJobs(request, jobId, checkedItemsStr);
            if (!"".equalsIgnoreCase(jobIdStr)) {
                message = new PilogUtilities().convertIntoMultilingualValue(labelObject, "Job updated succesfully") + " ";
            } else {
                message = "Error saving Job";
                message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObject, message);
            }
            resultObj.put("message", message);
            resultObj.put("jobId", jobIdStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public String updateGroupJobs(HttpServletRequest request, String jobId, String checkedItemsStr) {

        int insertJobCount = -1;
        String jobIdStr = "";

        try {

            insertJobCount = groupJobsDAO.updateGroupJobs(request, jobId, checkedItemsStr, 0);

            if (insertJobCount > 0) {
                jobIdStr = jobId;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobIdStr;
    }

    public String saveGroupJobs(HttpServletRequest request, String jobId, String jobName, JSONObject jobDetails, String checkedItemsStr) { //-----savejobsOpt

        int insertJobCount = -1;
        String jobIdStr = "";

        try {
            saveJobsDAO.deleteCDmJobs(request, jobId);
            insertJobCount = groupJobsDAO.saveGroupJobs(request, jobId, jobName, jobDetails, checkedItemsStr, 0);

            if (insertJobCount > 0) {
                jobIdStr = jobId;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobIdStr;
    }
}
