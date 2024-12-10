/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.pilog.mdm.DAO.iTransformTreeDataPipingDAO;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;



/**
 *
 * @author sanya
 */
@Service
public class iTransformTreeDataPipingService {
    
    @Autowired
    private iTransformTreeDataPipingDAO genericTreeDataPipingDAO;
    public JSONArray getTreePagingDataOpt(HttpServletRequest request, String parentkeyData) {

        return genericTreeDataPipingDAO.getTreePagingDataOpt(request, parentkeyData);
    }

}
