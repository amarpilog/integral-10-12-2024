/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;



import com.pilog.mdm.DAO.V10GenericDataMigrationDAO;
import com.pilog.mdm.DAO.V10GenericDataPipingDAO;
import com.pilog.mdm.DAO.V10DataMigrationAccessDAO;
import com.pilog.mdm.utilities.AuditIdGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *
 * @author sanya
 */
@Service
public class V10iVisionDataTransformationService {

    @Value("${file.storeFilePath}")
    private String storeFilePath;
    @Value("${MultipartResolver.fileUploadSize}")
    private long maxFileSize;
    @Value("${MultipartResolver.fileinMemorySize}")
    private int maxMemSize;
    @Value("${jdbc.batchSize}")
    private int batchSize;
    @Autowired
    private V10GenericDataMigrationService dataMigrationService;
    @Autowired
    private V10GenericProcessETLDataService genericProcessETLDataService;
    @Autowired
    private V10GenericDataPipingDAO genericDataPipingDAO;
    @Autowired
    private V10GenericDataMigrationDAO dataMigrationDAO;

    @Autowired
    private V10GenericProcessJobService processJobService;

    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;

    @Autowired
    private V10GenericDataPipingService dataPipingService;


    private V10DataMigrationAccessDAO dataMigrationAccessDAO = new V10DataMigrationAccessDAO();

    public JSONObject processJobData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        String jobId = request.getParameter("jobId");
        if (!(jobId != null
                && !"".equalsIgnoreCase(jobId)
                && !"null".equalsIgnoreCase(jobId))) {
            jobId = AuditIdGenerator.genRandom32Hex();
        }
        try {
            try {
                genericDataPipingDAO.deleteProcesslog((String) request.getSession(false).getAttribute("ssUsername"),
                        (String) request.getSession(false).getAttribute("ssOrgId"), jobId);
            } catch (Exception e) {
            }
            try {
                genericProcessETLDataService.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
                        (String) request.getSession(false).getAttribute("ssOrgId"), "ETL Process is started", "INFO", 20, "Y", jobId);
            } catch (Exception e) {
            }

            String mappedDataStr = request.getParameter("mappedData");
            String currentTrnsOpId = request.getParameter("currentTrnsOpId");
            if (mappedDataStr != null && !"".equalsIgnoreCase(mappedDataStr)) {
                resultObj = processJobService.processJobData((String) request.getSession(false).getAttribute("ssUsername"),
                        (String) request.getSession(false).getAttribute("ssOrgId"),
                        jobId,
                        mappedDataStr,
                        currentTrnsOpId);
//                Map mappedData = (Map) JSONValue.parse(mappedDataStr);
//                Map mappingOperatorObj = new HashMap();
//                Map nonMapOperatorObj = new HashMap();
//                if (mappedData != null && !mappedData.isEmpty()) {
//                    Map operatorsMap = (Map) mappedData.get("operators");
//                    if (operatorsMap != null && !operatorsMap.isEmpty()) {
//                        mappingOperatorObj = (Map) operatorsMap.keySet().stream()
//                                .filter(keyName -> (keyName != null && (Map) operatorsMap.get(keyName) != null
//                                && ("MAP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))
//                                || "GROUP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))
//                                || "UNGROUP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType"))))))
//                                .collect(Collectors.toMap(keyName -> keyName, keyName -> (Map) operatorsMap.get(keyName)));
//                        if (mappingOperatorObj != null && !mappingOperatorObj.isEmpty()) {
//                            resultObj = processJobService.processJobData((String) request.getSession(false).getAttribute("ssUsername"),
//                                    (String) request.getSession(false).getAttribute("ssOrgId"),
//                                    mappedData,
//                                    mappingOperatorObj,
//                                    jobId);
//
//                        }
//                    }
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                genericProcessETLDataService.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
                        (String) request.getSession(false).getAttribute("ssOrgId"),
                        "Getting error while Process :" + e.getMessage(), "ERROR", 20, "N", jobId);
            } catch (Exception ex) {
            }
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
        } finally {
            if (!(resultObj != null)) {
                resultObj = new JSONObject();
            }
            resultObj.put("jobId", jobId);
        }
        return resultObj;
    }

    // for job Schedular
    public JSONObject processJobData(String jobId,
            String sessionOrgnId,
            String sessionUserName,
            String processJobDataStr,
            String mappedDataStr,
            String parallelJobsDataStr
    ) {
        JSONObject resultObj = new JSONObject();
        if (!(jobId != null
                && !"".equalsIgnoreCase(jobId)
                && !"null".equalsIgnoreCase(jobId))) {
            jobId = AuditIdGenerator.genRandom32Hex();
        }
        try {
            try {
                genericDataPipingDAO.deleteProcesslog(sessionUserName,
                        sessionOrgnId, jobId);
            } catch (Exception e) {
            }
            try {
                genericProcessETLDataService.processETLLog(sessionUserName,
                        sessionOrgnId, "ETL Process is started", "INFO", 20, "Y", jobId);
            } catch (Exception e) {
            }

            if (mappedDataStr != null && !"".equalsIgnoreCase(mappedDataStr)) {
                Map mappedData = (Map) JSONValue.parse(mappedDataStr);
                Map mappingOperatorObj = new HashMap();
                Map nonMapOperatorObj = new HashMap();
                if (mappedData != null && !mappedData.isEmpty()) {
                    Map operatorsMap = (Map) mappedData.get("operators");
                    if (operatorsMap != null && !operatorsMap.isEmpty()) {
                        mappingOperatorObj = (Map) operatorsMap.keySet().stream()
                                .filter(keyName -> (keyName != null && (Map) operatorsMap.get(keyName) != null
                                && ("MAP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))
                                || "GROUP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType")))
                                || "UNGROUP".equalsIgnoreCase(String.valueOf(((Map) operatorsMap.get(keyName)).get("iconType"))))))
                                .collect(Collectors.toMap(keyName -> keyName, keyName -> (Map) operatorsMap.get(keyName)));
                        if (mappingOperatorObj != null && !mappingOperatorObj.isEmpty()) {
//                            resultObj = processJobService.processJobSchedularData(sessionUserName,
//                                    sessionOrgnId,
//                                    mappedData,
//                                    mappingOperatorObj,
//                                    jobId,mappedDataStr);
                            // ravi updated processjob for sceduling

                            resultObj = processJobService.processJobSchedularDataHib(sessionUserName,
                                    sessionOrgnId,
                                    mappedData,
                                    mappingOperatorObj,
                                    jobId, mappedDataStr);

                        }
                    }
                }
            } else {
                try {
                    genericProcessETLDataService.processETLLog(sessionUserName,
                            sessionOrgnId,
                            "internal error due to mapped object null", "ERROR", 20, "N", jobId);
                } catch (Exception ex) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                genericProcessETLDataService.processETLLog(sessionUserName,
                        sessionOrgnId,
                        "Getting error while Process :" + e.getMessage(), "ERROR", 20, "N", jobId);
            } catch (Exception ex) {
            }
            resultObj.put("Message", e.getMessage());
            resultObj.put("connectionFlag", "N");
        } finally {
            if (!(resultObj != null)) {
                resultObj = new JSONObject();
            }
            resultObj.put("jobId", jobId);
        }
        return resultObj;
    }
}
