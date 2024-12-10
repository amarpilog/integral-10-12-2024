/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;

import com.pilog.mdm.service.iVisionTransformConnectionTabsService;
import javax.servlet.http.HttpServletRequest;
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
public class iVisionTransformConnectionTabsController {
    @Autowired
    private iVisionTransformConnectionTabsService connectionTabsService;
    
    @RequestMapping(value = "/getAvaliableConnections", method = {RequestMethod.POST, RequestMethod.GET})
    public @ResponseBody
    JSONObject getAvaliableConnections(HttpServletRequest request) {
       
        JSONObject resultObj=new JSONObject();
        try {
            String divId = request.getParameter("divId");
            String level = request.getParameter("level");
            String fileType = request.getParameter("fileType");
            String connectionType = request.getParameter("connectionType");
            String connectionName = request.getParameter("connectionName");
            
            String schemaObjectType = request.getParameter("schemaObjectType");
            String connectionObj = request.getParameter("connectionObjStr");
            
            String filterValue = request.getParameter("filterValue");
            String startIndex = request.getParameter("startIndex");
            String limit = request.getParameter("limit");
            String instance = request.getParameter("instance");
            
            
            resultObj = connectionTabsService.getAvaliableConnections(request, divId, level, fileType, 
            			connectionType, schemaObjectType, connectionName, connectionObj,
            			filterValue, startIndex, limit, instance);
        } catch (Exception e) {
        }
        return resultObj;
    }
}
