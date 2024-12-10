/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;

import com.pilog.mdm.service.IVisionTransformProcessJobComponentsService;
import java.io.File;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

/**
 *
 * @author Ravindar.P
 */
@Controller
public class IVisionTransformProcessJobComponentsController {

    @Autowired
    private IVisionTransformProcessJobComponentsService processJobComponentsService;

    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;

    @Value("${MultipartResolver.fileUploadSize}")
    private long maxFileSize;
    @Value("${MultipartResolver.fileinMemorySize}")
    private int maxMemSize;

    @Value("${windows.server.logpath}")
    private String windowsServerLogPath;

    @Value("${linux.server.logpath}")
    private String linuxServerLogPath;

    @RequestMapping(value = "/processJobComponents", method = {RequestMethod.POST, RequestMethod.GET}, consumes = "multipart/form-data")
    public @ResponseBody JSONObject processJobComponents(
            @RequestParam(value = "files", required = false) List<MultipartFile> files,  // Handle files
            @RequestParam("currentConnObj") String currentConnObj,
            @RequestParam("flowchartData") String flowchartDataStr,                     // Flowchart data
            @RequestParam("jobId") String jobId,
            HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        try {
            request.setAttribute("currentConnObj", currentConnObj);
            request.setAttribute("files", files);
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            resultObject = processJobComponentsService.processJobComponents(request, flowchartData, jobId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    @RequestMapping(value = "/cancellProcessJob", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject cancellProcessJob(HttpServletRequest request) {
        JSONObject scheduledJobObj = new JSONObject();
        try {
            String sessionUserName = (String) request.getSession(false).getAttribute("ssUsername");
            String sessionOrgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            String jobId = request.getParameter("jobId");

            processJobComponentsService.cancellProcessJob(sessionUserName, sessionOrgnId, jobId);

        } catch (Exception e) {
        }
        return scheduledJobObj;
    }

    @RequestMapping(value = "/cancellAllRunningJobs", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject cancellAllRunningJobs(HttpServletRequest request) {
        JSONObject scheduledJobObj = new JSONObject();
        try {
            try {
                String sessionUserName = (String) request.getSession(false).getAttribute("ssUsername");
                String sessionOrgnId = (String) request.getSession(false).getAttribute("ssOrgId");
                String jobId = request.getParameter("jobId");

                processJobComponentsService.cancellAllRunningJobs(sessionUserName, sessionOrgnId, jobId);

            } catch (Exception e) {
            }
        } catch (Exception e) {
        }
        return scheduledJobObj;
    }

    @RequestMapping(value = "/ProcessGroupJobData", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject processGroupJobData(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            String jobId = request.getParameter("jobId");
            String flowchartDataStr = request.getParameter("flowchartData");
            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
            resultObj = processJobComponentsService.processGroupJobData(request, flowchartData, jobId);
        } catch (Exception e) {
        }
        return resultObj;
    }


    @RequestMapping(value = "/fetchServerConsoleLog", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject fetchServerConsoleLog(HttpServletRequest request) {
        String logStr = "";

        JSONObject resultObj = new JSONObject();

        try {
            String serverLogPath = "";

            if (System.getProperty("os.name")!=null && System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
                serverLogPath = windowsServerLogPath;
            } else {
                serverLogPath = linuxServerLogPath;
            }

            File file = new File(serverLogPath);
            int n_lines = 1000;
            int counter = 0;
            ReversedLinesFileReader object = new ReversedLinesFileReader(file);
            while (counter < n_lines) {
                String line = String.valueOf(object.readLine());
                if (line.indexOf("INFO") >0) {
                    // #00ff00; ->>light green
                    logStr =  "<span style='color:green;'>"+line+"</span>"+ "<br>"+logStr ;
                } else if (line.indexOf("WARN") >0) {
                    logStr =  "<span style='color:orange;'>"+line+"</span>"+ "<br>"+logStr ;
                } else if (line.indexOf("ERROR") >0) {
                    logStr =  "<span style='color:red;'>"+line+"</span>"+ "<br>"+logStr ;
                } else {
                    logStr =  "<span style='color:blue;'>"+line+"</span>"+ "<br>"+logStr ;
                }

                counter++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        resultObj.put("logStr", logStr);
        return resultObj;
    }


}
