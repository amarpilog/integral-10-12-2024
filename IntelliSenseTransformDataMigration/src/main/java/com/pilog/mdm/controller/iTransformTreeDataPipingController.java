/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;

import com.pilog.mdm.service.iTransformTreeDataPipingService;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;


/**
 *
 * @author sanya
 */
@Controller
public class iTransformTreeDataPipingController {
    @Autowired
    private iTransformTreeDataPipingService dataTreePipingService;
    @RequestMapping(value = "/getETLTreePagingDataOpt", method = {RequestMethod.GET, RequestMethod.POST}, produces = "application/json")
    public @ResponseBody
    JSONArray getTreePagingDataOpt(HttpServletRequest request) {
        JSONArray treeObj = new JSONArray();
        try {
            String parentkeyData = request.getParameter("parentkey");
            System.out.println("parentkey:::" + parentkeyData);
            treeObj = dataTreePipingService.getTreePagingDataOpt(request, parentkeyData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return treeObj;
    }
    
}
