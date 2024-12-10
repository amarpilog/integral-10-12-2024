package com.pilog.mdm.controller;

import com.pilog.mdm.service.IVisionTransformDebugJobService;
import javax.servlet.http.HttpServletRequest;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;



@Controller
public class IVisionTransformDebugJobController {
	
    @Autowired
    private IVisionTransformDebugJobService debugJobService;

  
    @RequestMapping(value = "/debugProcessJob", method = {RequestMethod.GET, RequestMethod.POST}, produces = "application/json;charset=UTF-8")
    public @ResponseBody
    JSONObject debugProcessJob(HttpServletRequest request) {
	        JSONObject resultObj = new JSONObject();
	        try {
	       
	        	String jobId = request.getParameter("jobId");
	            String flowchartDataStr = request.getParameter("flowchartData");
	            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
	            
	            resultObj = debugJobService.debugProcessJob(request, flowchartData, jobId);

	        } catch (Exception e) {
	        }
	        return resultObj;
	    }
}
