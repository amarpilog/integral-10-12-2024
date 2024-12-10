/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;

import com.pilog.mdm.service.iVisionTransformTableOperationsService;
import javax.servlet.http.HttpServletRequest;

import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import org.springframework.web.bind.annotation.RequestMethod;

/**
 *
 * @author Ravindar.P
 */
@Controller
public class iVisionTransformTableOperationsController {

    @Autowired
    private iVisionTransformTableOperationsService tableOperationsService;

    @RequestMapping(value = "/insertRecordsToTable", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject insertRecordsToTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            String treeId = request.getParameter("treeId");
            resultObj = tableOperationsService.insertRecordsToTable(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = "/updateRecordsInTable", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject updateRecordsInTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            String treeId = request.getParameter("treeId");
            resultObj = tableOperationsService.updateRecordsInTable(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = "/deleteRecordsInTable", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject deleteRecordsInTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            String treeId = request.getParameter("treeId");
            resultObj = tableOperationsService.deleteRecordsInTable(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = "/truncateTableData", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject truncateTableData(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {

            resultObj = tableOperationsService.truncateTableData(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = "/alterTable", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject alterTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {

            resultObj = tableOperationsService.alterTable(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = "/executeAlterTable", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject executeAlterTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {

            resultObj = tableOperationsService.executeAlterTable(request); // executeAlterTable
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
    
      @RequestMapping(value = "/generateAlterTableQuery", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject generateAlterTableQuery(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {

            resultObj = tableOperationsService.generateAlterTableQuery(request); // generateAlterTableQuery
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
    
       @RequestMapping(value = "/dropColumnAlterTable", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject dropColumnAlterTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = tableOperationsService.dropColumnAlterTable(request); // executeAlterTable
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
       
       
       @RequestMapping(value = "/updateColumnFuntion", method = {RequestMethod.GET, RequestMethod.POST})
       public @ResponseBody
       JSONObject updateColumnFuntion(HttpServletRequest request) {

           JSONObject resultObj = new JSONObject();
           try {
               resultObj = tableOperationsService.updateColumnFuntion(request); 
           } catch (Exception e) {
               e.printStackTrace();
           }
           return resultObj;
       }
       
       @RequestMapping(value = "/updateColumnReplace", method = {RequestMethod.GET, RequestMethod.POST})
       public @ResponseBody
       JSONObject updateColumnReplace(HttpServletRequest request) {

           JSONObject resultObj = new JSONObject();
           try {
               resultObj = tableOperationsService.updateColumnReplace(request); 
           } catch (Exception e) {
               e.printStackTrace();
           }
           return resultObj;
       }
       
       @RequestMapping(value = "/getTableScript", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject getTableScript(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
           String flag = request.getParameter("flag");
            if("S".equalsIgnoreCase(flag)){
                resultObj = tableOperationsService.getTableScript(request);
            } else if("T".equalsIgnoreCase(flag)){
                resultObj = tableOperationsService.getTableTriggers(request);
            } else if("F".equalsIgnoreCase(flag)){
                resultObj = tableOperationsService.getDataseFunctions(request);
            } else if("P".equalsIgnoreCase(flag)){
                resultObj = tableOperationsService.getDataseProcedures(request);
            } else if("O".equalsIgnoreCase(flag)){
                resultObj = tableOperationsService.enableOrDisableTrigger(request);
            }

        } catch (Exception e) {
        }
        return resultObj;
    }
    @RequestMapping(value = "/dropTable", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject dropTable(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {

            resultObj = tableOperationsService.dropTable(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

}
