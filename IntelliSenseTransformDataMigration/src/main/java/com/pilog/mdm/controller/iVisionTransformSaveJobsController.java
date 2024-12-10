/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;

import com.pilog.mdm.service.iVisionTransformSaveJobsService;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;


/**
 *
 * @author Ravindar.P
 */
@Controller
public class iVisionTransformSaveJobsController {
    
    @Autowired
    private iVisionTransformSaveJobsService saveJobsService;
    
      @RequestMapping(value = "/saveFolderName", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject saveFolderName(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.saveFolderName(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    @RequestMapping(value = "/deleteETLFolder", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject deleteETLFolder(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.deleteETLFolder(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    @RequestMapping(value = "/fetchSavedFolders", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject fetchSavedFolders(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.fetchSavedFolders(request);
//            resultObj = dataPipingService.fetchFolders(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    @RequestMapping(value = "/getFolderData", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject getFolderData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.getFolderData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
      @RequestMapping(value = "/getSavedJobData", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject getSavedJobData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.getSavedJobData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

    @RequestMapping(value = "/fetchSavedJobs", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject fetchSavedJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.fetchSavedJobs(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

        @RequestMapping(value = "/deleteJob", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject deleteJob(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.deleteJob(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
        @RequestMapping(value = "/saveMappings", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String saveMappings(HttpServletRequest request, HttpServletResponse response) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.saveMappings(request);
        } catch (Exception e) {
        }
        return resultObj.toJSONString();
    }
    
        
    @RequestMapping(value = "/getJobTransformationRules", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject getJobTransformationRules(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.getJobTransformationRules(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
    
    @RequestMapping(value = "/getSavedJobs", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject getSavedJobs(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.getSavedJobs(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
    @RequestMapping(value = "/filterSavedJobs", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject filterSavedJobs(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.filterSavedJobs(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
     @RequestMapping(value = "/renameETLFolder", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject renameETLFolder(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.renameETLFolder(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
    @RequestMapping(value = "/moveSavedJobs", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject moveSavedJobs(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = saveJobsService.moveSavedJobs(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    @RequestMapping(value = "/systemJobVerification", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject systemJobVerification(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            String folderName = request.getParameter("folderName");
            String folderId = request.getParameter("folderId");
            String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            if("DAB078D28DBDE1F3CE23DB39C0970A53".equalsIgnoreCase(folderId) && "System Jobs".equalsIgnoreCase(folderName)) {
                if("C1F5CFB03F2E444DAE78ECCEAD80D27D".equalsIgnoreCase(orgnId)) {
                    resultObj.put("accessMessage", "accessDone");
                } else {
                    resultObj.put("accessMessage", "you are not authorized to save job");
                }
            }else {
                resultObj.put("accessMessage", "accessDone");
            }
        } catch (Exception e) {
        }
        return resultObj;
    }

}
