/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;


import com.pilog.mdm.service.iVisionTransformationGroupJobsService;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 *
 * @author Uttej.K
 */
@Controller
public class iVisionTransformationGroupJobsController {

    @Autowired
    private iVisionTransformationGroupJobsService groupJobsService;

    @RequestMapping(value = "/fetchJobs", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject fetchJobs(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = groupJobsService.fetchJobs(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

    @RequestMapping(value = "/saveGroupjob", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject saveGroupjob(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = groupJobsService.saveGroupjob(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

    @RequestMapping(value = "/updateGroupjob", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject updateGroupjob(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            resultObj = groupJobsService.updateGroupjob(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

}
