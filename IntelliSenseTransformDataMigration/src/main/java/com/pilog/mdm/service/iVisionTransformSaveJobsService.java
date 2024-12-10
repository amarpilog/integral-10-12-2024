/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;


import com.pilog.mdm.DAO.V10DataMigrationAccessDAO;
import com.pilog.mdm.DAO.iVisionTransformSaveJobsDAO;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class iVisionTransformSaveJobsService {

    @Autowired
    private iVisionTransformSaveJobsDAO saveJobsDAO;

    private V10DataMigrationAccessDAO dataMigrationAccessDAO = new V10DataMigrationAccessDAO();

    public JSONObject getSavedJobData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsDAO.getSavedJobData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

    public JSONObject fetchDMSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            String dataModellerJobshtmlStr = "";
            List<Object[]> jobsList = saveJobsDAO.fetchSavedJobs(request, "DM");
            if (jobsList != null && !jobsList.isEmpty()) {
                dataModellerJobshtmlStr = "<div id=\"dataModellerJobsTree\">"
                        + "<ul>"
                        + "<li>Data Modeler Jobs"
                        + "<ul>";
                for (int i = 0; i < jobsList.size(); i++) {
                    Object[] jobArray = jobsList.get(i);
                    if (jobArray != null && jobArray.length != 0) {
                        dataModellerJobshtmlStr += "<li>"
                                + "<div id=\"" + jobArray[0] + "\" class=\"visionETLAvailableJobs\""
                                + " ondblclick=\"openSavedDataModellerJob(event,'" + jobArray[0] + "','" + jobArray[1] + "')\">"
                                + " <img src=\"images/Process Icon-01.svg\" class=\"visionETLIcons\""
                                + "  style=\"width:25px;height: 15px;cursor:pointer;\"/>"
                                + " <span style=\"cursor:pointer;\">"
                                + "" + String.valueOf(jobArray[1])
                                + "</span></div></li>"
                                + "";
                    }

                }
                dataModellerJobshtmlStr += "</ul></li></ul></div>";
            }

            resultObj.put("dataModellerJobshtmlStr", dataModellerJobshtmlStr);

        } catch (Exception e) {
        }
        return resultObj;
    }

    public JSONObject deleteJob(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
        int deleteCount = 0;
        String message = "";
        try {
            String jobId = request.getParameter("jobId");

            deleteCount = deleteCDmJobs(request, jobId); // ravi save new job

            if (deleteCount > 0) {
                message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObj, "Job Deleted Successfully");
            } else {
                message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObj, "Failed to delete the job");
            }
            resultObj.put("message", message);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject saveFolderName(HttpServletRequest request) {
        String message = "";
        JSONObject resultObj = new JSONObject();
        int insertCount = 0;
        try {
            String jobDetailStr = request.getParameter("jobDetails");
            JSONObject jobDetailObj = (JSONObject) JSONValue.parse(jobDetailStr);
            String jobType = (String) jobDetailObj.get("jobType");
            String treeValue = (String) jobDetailObj.get("treeValue");
            String folderName = request.getParameter("folderName");
            insertCount = saveJobsDAO.saveFolderName(request, jobType, treeValue, folderName);
            if (insertCount > 0) {
                message = "saved successfully";
            } else {
                message = "unable to save folder";
            }
            resultObj.put("message", message);
            resultObj.put("flag", true);
        } catch (Exception e) {
            resultObj.put("message", e.getMessage());
            resultObj.put("flag", false);
        }
        return resultObj;
    }

    public List<Object[]> getAllSubFoldersList(HttpServletRequest request, String folderName, String folderId, List totalSubFoldersList) {

        try {
            List<Object[]> getSubFoldersList = saveJobsDAO.getSubFoldersList(request, folderName, folderId);

            if (getSubFoldersList != null && !getSubFoldersList.isEmpty()) {
                totalSubFoldersList.addAll(getSubFoldersList);
                for (int i = 0; i < getSubFoldersList.size(); i++) {
                    Object[] folderObj = getSubFoldersList.get(i);
                    getAllSubFoldersList(request, String.valueOf(folderObj[1]), String.valueOf(folderObj[0]), totalSubFoldersList);

                }
            }
        } catch (Exception e) {
        }
        return totalSubFoldersList;
    }

    public JSONObject deleteETLFolder(HttpServletRequest request) {
        String message = "";
        JSONObject resultObj = new JSONObject();
        List<Object[]> totalSubFoldersList = new ArrayList();
        int deleteCount = 0;
        try {

            String folderName = request.getParameter("folderName");
            String folderId = request.getParameter("folderId");
            totalSubFoldersList = getAllSubFoldersList(request, folderName, folderId, totalSubFoldersList);

            if (totalSubFoldersList != null && !totalSubFoldersList.isEmpty()) {
                for (int i = 0; i < totalSubFoldersList.size(); i++) {
                    Object[] folderObj = totalSubFoldersList.get(i);
                    String foldId = (String) folderObj[0];
                    String foldName = (String) folderObj[1];

                    if (foldId != null && foldName != null && !"".equalsIgnoreCase(folderId) && !"null".equalsIgnoreCase(folderId)) {
                        List<Object[]> getSubFolderJobs = saveJobsDAO.getSubFolderJobs(request, String.valueOf(folderObj[1]), String.valueOf(folderObj[0]));
                        if (getSubFolderJobs != null && !getSubFolderJobs.isEmpty()) {
                            for (int j = 0; j < getSubFolderJobs.size(); j++) {
                                Object[] jobObj = getSubFolderJobs.get(j);
                                String jobName = (String) jobObj[1];
                                if (jobName != null && !"".equalsIgnoreCase(jobName) && !"null".equalsIgnoreCase(jobName) && jobName != "PARENT_ID" && !"PARENT_ID".equalsIgnoreCase(jobName)) {
                                    saveJobsDAO.deleteFolderJobs(request, String.valueOf(jobObj[1]), String.valueOf(jobObj[0]));
                                }
                            }
                        }
                        deleteCount += saveJobsDAO.deleteETLFolder(request, String.valueOf(folderObj[1]), String.valueOf(folderObj[0]));
                    }
                }
            }
            List<Object[]> getFolderJobs = saveJobsDAO.getSubFolderJobs(request, folderName, folderId);
            if (getFolderJobs != null && !getFolderJobs.isEmpty()) {
                for (int j = 0; j < getFolderJobs.size(); j++) {
                    Object[] jobObj = getFolderJobs.get(j);
                    String jobName = (String) jobObj[1];
                    if (jobName != null && !"".equalsIgnoreCase(jobName) && !"null".equalsIgnoreCase(jobName) && jobName != "PARENT_ID" && !"PARENT_ID".equalsIgnoreCase(jobName)) {
                        saveJobsDAO.deleteFolderJobs(request, String.valueOf(jobObj[1]), String.valueOf(jobObj[0]));
                    }
                }
            }
//            genericDataPipingDAO.deleteFolderJobs(request, folderName, folderId);
            deleteCount += saveJobsDAO.deleteETLFolder(request, folderName, folderId);

            if (deleteCount > 0) {
                message = "Folder deleted successfully";
            } else {
                message = "unable to delete folder";
            }
            resultObj.put("message", message);
            resultObj.put("flag", true);
        } catch (Exception e) {
            resultObj.put("message", e.getMessage());
            resultObj.put("flag", false);
        }
        return resultObj;
    }

    public JSONObject getFolderData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            String parentFolder = request.getParameter("parentFolder");
            String parentFolderId = request.getParameter("parentFolderId");
            String jobType = request.getParameter("jobType");
            JSONArray availableFoldersArray = new JSONArray();
            JSONArray availableJobsArray = new JSONArray();

            List<Object[]> foldersList = saveJobsDAO.fetchfoldersList(request, jobType, parentFolder, parentFolderId);
            if (foldersList != null && !foldersList.isEmpty()) {

                for (int i = 0; i < foldersList.size(); i++) {
                    Object[] folderObj = foldersList.get(i);
                    availableFoldersArray.add("<div class ='visionEtlTreeFolders' id ='" + folderObj[0] + "' style='display: inline-flex;'><img src=\"images/File-Icon.svg\" class=\"visionETLIcons\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + folderObj[1] + "</div>");
                }
            }
            List<Object[]> fetchjobsList = saveJobsDAO.fetchJobsListInFolder(request, jobType, parentFolder, parentFolderId);

            for (int j = 0; j < fetchjobsList.size(); j++) {
                Object[] jobArray = fetchjobsList.get(j);
                if (jobArray != null && jobArray.length != 0) {
                    String jobValue = String.valueOf(jobArray[1]);
                    if (jobValue != "PARENT_ID" && !"PARENT_ID".equalsIgnoreCase(jobValue)) {
                        String itemHtml = "<div id=\"" + jobArray[0] + "\" class=\"visionETLAvailableJobs\""
                                + " ondblclick=\"openSavedJob(event,'" + jobArray[0] + "','" + jobArray[1] + "','" + parentFolder + "','" + parentFolderId + "')\">"
                                + "<img src=\"images/Process Icon-01.svg\" class=\"visionETLIcons\""
                                + "  style=\"width:25px;height: 15px;cursor:pointer;\"/>"
                                + "<span style=\"cursor:pointer;\">"
                                //                                + "" + String.valueOf(jobArray[1]).toUpperCase()
                                + "" + String.valueOf(jobArray[1])
                                + "</span>"
                                + "</div>";
                        availableJobsArray.add(itemHtml);
                    }
                }
            }

            resultObj.put("availableFoldersArray", availableFoldersArray);
            resultObj.put("availableJobsArray", availableJobsArray);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject fetchSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

//            resultObj = fetchDMSavedJobs(request);
            String availableJobshtmlStr = "";
            String parentFolder = request.getParameter("parentFolder");
            String parentFolderId = request.getParameter("parentFolderId");
//            List<Object[]> foldersList = saveJobsDAO.fetchfoldersList(request, "ETL", parentFolder, parentFolderId);
//            if (foldersList != null && !foldersList.isEmpty()) {
            availableJobshtmlStr = "<div id='avaialableJobsTree'>"
                    + "<ul>"
                    + "<li><div id ='C8DE75F32D56288CE0554B610B40A4A3' class ='visionEtlTreeFolders'  style='display: inline-flex;'><img src=\"images/File-Icon.svg\" class=\"visionETLIcons\" style=\"width:15px;height: 15px;cursor:pointer;\"/>Saved Jobs</div>"
                    + "<ul><li><div>loading...</div></li></ul>"
                    + "</li>"
                    + "</ul>";

//                for (int i = 0; i < foldersList.size(); i++) {
//                    Object[] folderObj = foldersList.get(i);
//                    String folderId = (String) folderObj[0];
//                    String folderName = (String) folderObj[1];
//                    if (folderId != null && folderName != null && !"".equalsIgnoreCase(folderId) && !"".equalsIgnoreCase(folderName) && !"null".equalsIgnoreCase(folderId) && !"null".equalsIgnoreCase(folderId)) {
//                        availableJobshtmlStr += "<li><div id ='" + folderObj[0] + "' class ='visionEtlTreeFolders' style='display: inline-flex;'><img src=\"images/File-Icon.svg\" class=\"visionETLIcons\" style=\"width:15px;height: 15px;cursor:pointer;\"/>" + folderObj[1] + "</div>"
//                                + "<ul>"
//                                + "<li style='display:none'></li>"
//                                + "</ul>"
//                                + "</li>";
//                    }
//                }
//            }
//            if (foldersList.isEmpty()) {
//                availableJobshtmlStr = "<div id='avaialableJobsTree'>"
//                        + "<ul>"
//                        + "<li><div id ='C8DE75F32D56288CE0554B610B40A4A3' class ='visionEtlTreeFolders' style='display: inline-flex;'><img src=\"images/File-Icon.svg\" class=\"visionETLIcons\" style=\"width:15px;height: 15px;cursor:pointer;\"/>Saved Jobs</div>"
//                        + "<ul>"
//                        + "<li style='display:none'></li>";
////                        + "</ul></li>";
//            }
//            List<Object[]> fetchjobsList = saveJobsDAO.fetchJobsListInFolder(request, "ETL", parentFolder, parentFolderId);
//            for (int i = 0; i < fetchjobsList.size(); i++) {
//                Object[] jobArray = fetchjobsList.get(i);
//                String jobId = (String) jobArray[0];
//                String jobDesc = (String) jobArray[1];
////                    availableJobshtmlStr = "<li>"+jobDesc+"</li>";
//                availableJobshtmlStr += "<li>"
//                        + "<div id=\"" + jobArray[0] + "\" class=\"visionETLAvailableJobs\""
//                        + " ondblclick=\"openSavedJob(event,'" + jobArray[0] + "','" + jobArray[1] + "','" + parentFolder + "','" + parentFolderId + "')\">"
//                        + "<img src=\"images/Process Icon-01.svg\" class=\"visionETLIcons\""
//                        + "  style=\"width:15px;height: 15px;cursor:pointer;\"/>"
//                        + "<span style=\"cursor:pointer;\">"
//                        + "" + String.valueOf(jobArray[1]).toUpperCase()
//                        + "</span>"
//                        + "</div>"
//                        + "</li>";
//            }
//            availableJobshtmlStr += "</ul></li></ul></li></ul></li></ul></div>";
            //uttej code
            String predefinedJobshtmlStr = "";
            List<Object[]> predefinedJobsList = saveJobsDAO.fetchSavedJobs(request, "Y");
            if (predefinedJobsList != null && !predefinedJobsList.isEmpty()) {
                predefinedJobshtmlStr = "<div id=\"predefinedJobsTree\">"
                        + "<ul>"
                        + "<li>Predefined Jobs"
                        + "<ul>";
                for (int i = 0; i < predefinedJobsList.size(); i++) {
                    Object[] jobArray = predefinedJobsList.get(i);
                    if (jobArray != null && jobArray.length != 0) {
                        predefinedJobshtmlStr += "<li>"
                                + "<div id=\"" + jobArray[0] + "\" class=\"visionETLAvailableJobs\""
                                + " ondblclick=\"openPredefinedJob(event,'" + jobArray[0] + "','" + jobArray[1] + "')\">"
                                + "<img src=\"images/Process Icon-01.svg\" class=\"visionETLIcons\""
                                + "  style=\"width:25px;height: 15px;cursor:pointer;\"/>"
                                + "<span style=\"cursor:pointer;\">"
                                + "" + String.valueOf(jobArray[1])
                                + "</span></div></li>"
                                + "";
                    }

                }
                predefinedJobshtmlStr += "</ul></div>";
            }

            resultObj.put("availableJobshtmlStr", availableJobshtmlStr);
            resultObj.put("predefinedJobshtmlStr", predefinedJobshtmlStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public JSONObject fetchSavedFolders(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            JSONArray foldersArr = new JSONArray();
            JSONObject foldersObj = new JSONObject();
            List<Object[]> getGroupByValue = saveJobsDAO.fetchSavedGroupBy(request, "ETL");
            if (getGroupByValue != null && !getGroupByValue.isEmpty()) {
                String parentFolder = request.getParameter("parentFolder");
                String parentId = request.getParameter("parentId");
                foldersObj.put("id", parentFolder);
                foldersObj.put("text", parentFolder);
                foldersObj.put("value", parentId);
                foldersObj.put("icon", "images/File-Icon.svg");
                foldersArr.add(foldersObj);
                for (int i = 0; i < getGroupByValue.size(); i++) {
                    Object[] folArr = getGroupByValue.get(i);
                    String folderId = (String) folArr[0];
                    String folderName = (String) folArr[1];
                    List<Object[]> getChildTreeData = saveJobsDAO.getChildTreeData(request, "ETL", folderId, folderName);
                    if (getChildTreeData != null && !getChildTreeData.isEmpty()) {
                        for (int j = 0; j < getChildTreeData.size(); j++) {
                            Object[] childFolArr = getChildTreeData.get(j);
                            String childFolder = (String) childFolArr[1];
                            if (childFolder != null && !"".equalsIgnoreCase(childFolder) && !"null".equalsIgnoreCase(childFolder)) {
                                foldersObj = new JSONObject();
                                foldersObj.put("id", childFolder);
                                foldersObj.put("text", childFolder);
                                foldersObj.put("value", String.valueOf(childFolArr[0]));
                                foldersObj.put("icon", "images/File-Icon.svg");
                                foldersObj.put("parentid", "" + folderName);
                                foldersArr.add(foldersObj);
                            }
                        }
                    } else {
                        if (folderName != null && !"".equalsIgnoreCase(folderName) && !"null".equalsIgnoreCase(folderName)) {
                            foldersObj = new JSONObject();
                            foldersObj.put("id", folderName);
                            foldersObj.put("text", folderName);
                            foldersObj.put("value", String.valueOf(folArr[0]));
                            foldersObj.put("icon", "images/File-Icon.svg");
                            foldersObj.put("parentid", "FOLDER_NAME");
                            foldersArr.add(foldersObj);
                        }
                    }

                }

            }
            resultObj.put("scheduledJobsArray", foldersArr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public String copyJob(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        //JSONObject trfmRulesObj = new JSONObject();
        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);
        int insertCount = 0;
        int insertJobCount = 0;
        String message = "";
        try {
            String jobId = request.getParameter("jobId");
            String jobName = request.getParameter("jobName");
            String jobDetailsStr = request.getParameter("jobDetails");

            JSONObject jobDetails = new JSONObject();
            if (jobDetailsStr != null && !"".equalsIgnoreCase(jobDetailsStr)) {
                jobDetails = (JSONObject) JSONValue.parse(jobDetailsStr);
                String folderId = request.getParameter("folderId");
                String folderName = request.getParameter("folderName");
                jobDetails.put("folderId", folderId);
                jobDetails.put("folderName", folderName);
            }

            List<Object[]> cDmJobsList = saveJobsDAO.getcDmJobsList(request, jobId);
            String[] trfmRuleIds = new String[cDmJobsList.size()];
            for (int i = 0; i < cDmJobsList.size(); i++) {
                trfmRuleIds[i] = AuditIdGenerator.genRandom32Hex();
            }

            if (cDmJobsList != null && !cDmJobsList.isEmpty()) {
                for (int i = 0; i < cDmJobsList.size(); i++) {
                    String trfmRulesId = (String) cDmJobsList.get(i)[5];
                    String newTrfmRulesId = trfmRuleIds[i];
                    String newJobId = AuditIdGenerator.genRandom32Hex();

                    String processJobData = new PilogUtilities().clobToString((Clob) cDmJobsList.get(i)[7]);
                    String mappedData = new PilogUtilities().clobToString((Clob) cDmJobsList.get(i)[6]);

                    JSONObject mappedDataObj = (JSONObject) JSONValue.parse(mappedData);
                    JSONObject operators = (JSONObject) mappedDataObj.get("operators");

                    for (int key = 0; key < operators.size(); key++) {
                        JSONObject operator = (JSONObject) operators.get(String.valueOf(key));
                        String targetOperator = (String) operator.get("targetOperator");
                        if (targetOperator != null && "Y".equalsIgnoreCase(targetOperator)) {
                            operator.put("trfmRulesId", newTrfmRulesId);
                            operator.put("jobName", jobName);
                            operator.put("jobId", newJobId);
                            operators.put(String.valueOf(key), operator);
                        }
                    }

                    mappedDataObj.put("operators", operators);
                    mappedData = mappedDataObj.toJSONString();

                    insertJobCount = saveJobsDAO.cDmJobs(request, new JSONObject(), mappedData, newTrfmRulesId, newJobId, jobName, i, processJobData, jobDetails);

                }
            }
            if (insertJobCount > 0) {
                message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObj, "Job Copied succesfully");

            } else {
                message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObj, "Error while Copying");
            }

        } catch (Exception e) {
            e.printStackTrace();
            message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObj, "Error while Copying");

        }
        return message;
    }

    public String renameJob(HttpServletRequest request) {

        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);

        String message = "";
        try {
            String jobId = request.getParameter("jobId");
            String jobName = request.getParameter("jobName");
            message = saveJobsDAO.renameJob(request, jobId, jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    public int deleteTrfmRules(HttpServletRequest request, String trfmRulesId) { // ravi transformation rules
        int deleteCount = 0;
        String jobIdStr = "";
        try {

            deleteCount = saveJobsDAO.deleteCDmJobs(request, trfmRulesId);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return deleteCount;
    }

    public JSONObject getJobTransformationRules(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsDAO.getJobTransformationRules(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

    public JSONObject saveMappings(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        String message = "";
        String jobIdStr = "";
        try {
            JSONObject labelObject = new PilogUtilities().getMultilingualObject(request);
            String jobName = request.getParameter("jobName");
            String jobId = request.getParameter("jobId"); // ravi save new job
            String jobType = request.getParameter("jobType"); // ravi save new job

            String jobDetailsStr = request.getParameter("jobDetails");
            JSONObject jobDetails = (JSONObject) JSONValue.parse(jobDetailsStr);

            String folderName = (String) request.getParameter("folderName");
            String folderId = (String) request.getParameter("folderId");
            jobDetails.put("folderName", folderName);
            jobDetails.put("folderId", folderId);

            if (jobId == null) {
                jobId = AuditIdGenerator.genRandom32Hex();
            }

            jobIdStr = saveJobs(request, jobId, jobName, jobDetails);
            // }
            if (!"".equalsIgnoreCase(jobIdStr)) {
                message = new PilogUtilities().convertIntoMultilingualValue(labelObject, "Job updated succesfully with") + " " + jobName;
            } else {
                message = "Error saving Job";
                message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObject, message);
            }

            resultObj.put("message", message);
            resultObj.put("jobId", jobIdStr);
            resultObj.put("jobName", jobName);
            resultObj.put("flowChartData", saveJobsDAO.getflowChartData(request, jobId));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public String saveJobs(HttpServletRequest request, String jobId, String jobName, JSONObject jobDetails) { //-----savejobsOpt

        int insertJobCount = -1;
        int insertCount = -1;
        String jobIdStr = "";

        try {
            JSONArray trfmRulesArray = new JSONArray();
            JSONObject mappedData = new JSONObject();
            String mappedDataStr = request.getParameter("mappedData");
            String processJobData = request.getParameter("processJobData");

            if (mappedDataStr != null && !"".equalsIgnoreCase(mappedDataStr)) {
                mappedData = (JSONObject) JSONValue.parse(mappedDataStr);
            }

            if (mappedData != null && !mappedData.isEmpty()) {

                Map<String, JSONObject> operatorsMap = (Map) mappedData.get("operators");

                JSONObject value = new JSONObject();
                for (Map.Entry<String, JSONObject> entry : operatorsMap.entrySet()) {
                    String key = entry.getKey();
                    value = entry.getValue();
                    String targetOperator = (String) value.get("targetOperator");
                    if (targetOperator != null && !"".equalsIgnoreCase(targetOperator)
                            && (targetOperator.equalsIgnoreCase("Y"))) {

                        //   break;
                        if (value.get("trfmRulesId") == null) {
                            String trfmRulesId = AuditIdGenerator.genRandom32Hex();
                            value.put("trfmRulesId", trfmRulesId);
                            ((JSONObject) operatorsMap.get(key)).put("trfmRulesId", trfmRulesId);
                            ((JSONObject) operatorsMap.get(key)).put("jobId", jobId);
                            ((JSONObject) operatorsMap.get(key)).put("jobName", jobName);
                            //trfmRulesArray.add(value);

                        } else {
                            String trfmRulesId = (String) value.get("trfmRulesId");

                            // trfmRulesArray.add(value);
                        }
                    }
                }
                mappedData.put("operators", operatorsMap);
            }

            String trfmRulesId = AuditIdGenerator.genRandom32Hex();
            JSONObject trfmRulesObj = new JSONObject();
            Map mappingOperatorObj = new HashMap();
            deleteCDmJobs(request, jobId); // ravi save new job
            insertJobCount = saveJobsDAO.cDmJobs(request, trfmRulesObj, mappedData.toString(), trfmRulesId, jobId, jobName, 0, processJobData, jobDetails);

            if (insertJobCount > 0) {
                jobIdStr = jobId;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobIdStr;
    }

    public int deleteCDmJobs(HttpServletRequest request, String jobId) { // ravi save new job
        int deleteCount = 0;
        String jobIdStr = "";
        try {

            deleteCount = saveJobsDAO.deleteCDmJobs(request, jobId);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return deleteCount;
    }
    
    
   public JSONObject getSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
 
        try {
            
            resultObj = saveJobsDAO.getSavedJobs(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
   
   public JSONObject filterSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
 
        try {
            
            resultObj = saveJobsDAO.filterSavedJobs(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
   
    public JSONObject renameETLFolder(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
 
        try {
            
            resultObj = saveJobsDAO.renameETLFolder(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
    
    public JSONObject moveSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
 
        try {
            
            resultObj = saveJobsDAO.moveSavedJobs(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    public String shareSavedJob(HttpServletRequest request) {

        JSONObject labelObj = new PilogUtilities().getMultilingualObject(request);

        String message = "";
        try {
            String jobId = request.getParameter("jobId");
            String username = request.getParameter("shareUsername");
            message = saveJobsDAO.shareSavedJob(request, jobId, username);
            message = "" + new PilogUtilities().convertIntoMultilingualValue(labelObj, message);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }
   

}
