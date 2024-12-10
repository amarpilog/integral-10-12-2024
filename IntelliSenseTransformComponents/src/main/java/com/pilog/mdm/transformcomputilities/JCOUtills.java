/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.transformcomputilities;

import com.sap.mw.jco.IRepository;
import com.sap.mw.jco.JCO;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Devint01
 */
public class JCOUtills {
    public IRepository retrieveRepository(JCO.Client connection) {
        IRepository theRepository = null;
        try {
            theRepository = new JCO.Repository("SAPJCOREP", connection);
        } catch (Exception ex) {
            System.out.println("failed to retrieve repository");
        }
        return theRepository;
    }
    public JCO.Function getFunction(String name,IRepository theRepository) {
        try {
            return theRepository.getFunctionTemplate(name.toUpperCase()).getFunction();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
    public  JCO.Client getSAPConnection(String ClientId,
            String userName,
            String password,
            String LanguageId,
            String hostName,
            String ERPSystemId
    ) {
        JCO.Client theConnection = null;
     
        try {
            theConnection = JCO.createClient(
                    ClientId,
                    userName,
                    password,
                    LanguageId,
                    hostName,
                    ERPSystemId);
            theConnection.connect();
//            returnObj = theConnection;
            System.out.println(" Succesfully connect to SAP system");
            //System.out.println("connection attribute:"+ theConnection.getAttributes());
        } catch (Exception ex) {
            ex.printStackTrace();
//            returnObj = ex.getMessage();
            System.out.println("Failed to connect to SAP system");
        }

        return theConnection;

    }
   public List<String> getColumnsInTable(JCO.Table table){
       List<String> tableFeilds = new ArrayList<>();
       try {
           JCO.FieldIterator fitr = table.fields();
           while (fitr.hasMoreFields()) {
               JCO.Field fields = fitr.nextField();
               if (fields != null) {
                   String fetchColumnName = fields.getName();
                   tableFeilds.add(fetchColumnName);
               }
               
           }
       } catch (Exception e) {
           e.printStackTrace();
       }
       return tableFeilds;
   }
}
