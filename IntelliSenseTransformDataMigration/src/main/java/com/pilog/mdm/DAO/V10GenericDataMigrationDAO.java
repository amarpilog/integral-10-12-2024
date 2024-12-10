/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;


import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;
import com.pilog.mdm.pojo.DalDmSavedConnections;
import com.pilog.mdm.pojo.DalDmSavedConnectionsId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author PiLog
 */
@Repository
public class V10GenericDataMigrationDAO {

    @Autowired
    private DataAccess access;

    @Transactional
    public String saveConnectionDetails(HttpServletRequest request) {
        String result = "";
        try {
            DalDmSavedConnections dalDmSavedConnections = new DalDmSavedConnections();
            DalDmSavedConnectionsId savedConnectionsId = new DalDmSavedConnectionsId();
            savedConnectionsId.setConnectionName((request.getParameter("connectionName") != null && !"".equalsIgnoreCase(request.getParameter("connectionName"))) ? request.getParameter("connectionName") : "");
            savedConnectionsId.setOrgnId( request.getSession(false).getAttribute("ssOrgId")!=null ? (String)request.getSession(false).getAttribute("ssOrgId") : "C1F5CFB03F2E444DAE78ECCEAD80D27D");
            savedConnectionsId.setUserName((String) request.getSession(false).getAttribute("ssUsername"));
            savedConnectionsId.setConnUserName((request.getParameter("userName") != null && !"".equalsIgnoreCase(request.getParameter("userName"))) ? request.getParameter("userName") : "");
            dalDmSavedConnections.setId(savedConnectionsId);
            dalDmSavedConnections.setHostName((request.getParameter("hostName") != null && !"".equalsIgnoreCase(request.getParameter("hostName"))) ? request.getParameter("hostName") : "");
            dalDmSavedConnections.setConnPort((request.getParameter("port") != null && !"".equalsIgnoreCase(request.getParameter("port"))) ? request.getParameter("port") : "");
            
            dalDmSavedConnections.setConnPassword((request.getParameter("password") != null && !"".equalsIgnoreCase(request.getParameter("password"))) ? request.getParameter("password") : "");
            dalDmSavedConnections.setConnDbName((request.getParameter("serviceName") != null && !"".equalsIgnoreCase(request.getParameter("serviceName"))) ? request.getParameter("serviceName") : "");
            dalDmSavedConnections.setCreateBy(((String) request.getSession(false).getAttribute("ssUsername") != null && !"".equalsIgnoreCase((String) request.getSession(false).getAttribute("ssUsername"))) ? (String) request.getSession(false).getAttribute("ssUsername") : "");
            dalDmSavedConnections.setCreateDate(new Date());
            dalDmSavedConnections.setEditBy(((String) request.getSession(false).getAttribute("ssUsername") != null && !"".equalsIgnoreCase((String) request.getSession(false).getAttribute("ssUsername"))) ? (String) request.getSession(false).getAttribute("ssUsername") : "");
            dalDmSavedConnections.setEditDate(new Date());
            dalDmSavedConnections.setConnCustCol1((request.getParameter("selectedItemLabel") != null && !"".equalsIgnoreCase(request.getParameter("selectedItemLabel"))) ? request.getParameter("selectedItemLabel") : "");
            dalDmSavedConnections.setAuditId(AuditIdGenerator.getAuditId("DAL_DM_SAVED_CONNECTIONS"));
            dalDmSavedConnections.setConnCustCol2((request.getParameter("ConnectionType") != null && !"".equalsIgnoreCase(request.getParameter("ConnectionType"))) ? request.getParameter("ConnectionType") : "");

            access.saveObj(dalDmSavedConnections);
            result = "true";
        } catch (Exception e) {

            e.printStackTrace();
            result = "false";
        }
        return result;
    }

    @Transactional
    public List getSavedConnections(HttpServletRequest request) {
        List connectionsList = new ArrayList();
        try {
            String query = "SELECT CONNECTION_NAME,"//0
                    + " HOST_NAME,"//1
                    + "CONN_PORT, "//2
                    + "CONN_USER_NAME, "//3
                    + "CONN_PASSWORD,"//4
                    + "CONN_DB_NAME,"//5
                    + "CONN_CUST_COL1,"//6
                    + "AUDIT_ID "//8
                    + " FROM DAL_DM_SAVED_CONNECTIONS "
                    + "WHERE ORGN_ID = :ORGN_ID"
                    + " AND CONN_CUST_COL2 = :CONN_CUST_COL2 AND USER_NAME =:USER_NAME";
            System.out.println("query::" + query);
            Map connectionsMap = new HashMap();
            connectionsMap.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            connectionsMap.put("CONN_CUST_COL2", "DB");
            connectionsMap.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            connectionsList = access.sqlqueryWithParams(query, connectionsMap);
            System.out.println("connectionsList::" + connectionsList);

        } catch (Exception e) {
        }
        return connectionsList;
    }

    @Transactional
    public String updateConnectionDetails(HttpServletRequest request) {
        String result = "";
        try {
            String updateQuery = "UPDATE DAL_DM_SAVED_CONNECTIONS SET CONNECTION_NAME = :CONNECTION_NAME,HOST_NAME =:HOST_NAME,CONN_PORT = :CONN_PORT,"
                    + "CONN_USER_NAME =:CONN_USER_NAME,CONN_PASSWORD = :CONN_PASSWORD,CONN_DB_NAME =:CONN_DB_NAME WHERE AUDIT_ID = :AUDIT_ID";
            System.out.println("query::" + updateQuery);
            Map updateConnectionsMap = new HashMap();
            updateConnectionsMap.put("CONNECTION_NAME", request.getParameter("connectionName"));
            updateConnectionsMap.put("HOST_NAME", request.getParameter("hostName"));
            updateConnectionsMap.put("CONN_PORT", request.getParameter("port"));
            updateConnectionsMap.put("CONN_USER_NAME", request.getParameter("userName"));
            updateConnectionsMap.put("CONN_PASSWORD", request.getParameter("password"));
            updateConnectionsMap.put("CONN_DB_NAME", request.getParameter("serviceName"));
            updateConnectionsMap.put("AUDIT_ID", request.getParameter("auditId"));
            int updateCount = access.executeUpdateSQL(updateQuery, updateConnectionsMap);
            if (updateCount != 0) {
                result = "Connection updated successfully";
            }

        } catch (Exception e) {
            e.printStackTrace();
            result = "Failed to update";
        }
        return result;
    }
//    @Transactional
//    public String deleteConnectionDetails(HttpServletRequest request){
//        String result = "";
//        try {
//            // ravi start 
//            String auditId = request.getParameter("auditId");
//            String connectionName = request.getParameter("connectionName");
//            if (!(auditId != null && !"".equalsIgnoreCase(auditId) && !"null".equalsIgnoreCase(auditId))
//                    && connectionName != null) {
//                List<Object[]> connDetailsList = getConDetails(request, connectionName);
//                if (connDetailsList != null && !connDetailsList.isEmpty()) {
//                    auditId = (String) connDetailsList.get(0)[9];
//                }
//            }
//            String deleteQuery = "DELETE DAL_DM_SAVED_CONNECTIONS WHERE AUDIT_ID = :AUDIT_ID";
//                System.out.println("query::" + deleteQuery);
//                Map deleteConnectionsMap = new HashMap();
//                 deleteConnectionsMap.put("AUDIT_ID", request.getParameter("auditId"));
//                 
//            int deleteCount = access.executeUpdateSQL(deleteQuery, deleteConnectionsMap);
//                  if(deleteCount != 0){
//                    result = "Connection deleted successfully"  ;
//                  }
//             
//             
//        } catch (Exception e) {
//            e.printStackTrace();
//         result = "Failed to delete";
//        }
//        return result;
//    }

    @Transactional
    public JSONObject deleteConnectionDetails(HttpServletRequest request) { // ravi etl integration
        JSONObject resultObj = new JSONObject();
        JSONObject labelObject = new PilogUtilities().getMultilingualObject(request);
        String result = "";
        try {
            
            String connectionName = request.getParameter("connectionName");
            
            String deleteQuery = "DELETE DAL_DM_SAVED_CONNECTIONS WHERE CONNECTION_NAME=:CONNECTION_NAME";
            System.out.println("query::" + deleteQuery);
            Map deleteConnectionsMap = new HashMap();
//                 deleteConnectionsMap.put("AUDIT_ID", request.getParameter("auditId"));
            deleteConnectionsMap.put("CONNECTION_NAME", connectionName); // ravi delete connection

            int deleteCount = access.executeUpdateSQL(deleteQuery, deleteConnectionsMap);
            if (deleteCount != 0) {
                result = "Connection deleted successfully";
                result = "" + new PilogUtilities().convertIntoMultilingualValue(labelObject, result);
                resultObj.put("deleteSucessful", "Y");
            }

        } catch (Exception e) {
            e.printStackTrace();
            result = "Failed to delete";
            result = "" + new PilogUtilities().convertIntoMultilingualValue(labelObject, result);
        }
        resultObj.put("message", result);

        return resultObj;
    }

    @Transactional
    public List getSavedErpConnections(HttpServletRequest request) {
        List connectionsList = new ArrayList();
        try {
            String query = "SELECT CONNECTION_NAME,"//0
                    + "HOST_NAME,"//1
                    + "CONN_PORT, "//2
                    + "CONN_USER_NAME, "//3
                    + "CONN_PASSWORD,"//4
                    + "CONN_DB_NAME,"//5
                    + "CONN_CUST_COL1,"//6
                    + "CONN_CUST_COL3,"//7
                    + "AUDIT_ID "//8
                    + "FROM DAL_DM_SAVED_CONNECTIONS WHERE "
                    + "ORGN_ID = :ORGN_ID AND "
                    + "CONN_CUST_COL2 = :CONN_CUST_COL2 AND USER_NAME =:USER_NAME";
            System.out.println("query::" + query);
            Map connectionsMap = new HashMap();
            connectionsMap.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));            //18-02
            connectionsMap.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));            //18-02
            connectionsMap.put("CONN_CUST_COL2", "ERP");
            System.out.println("query::" + query);
            connectionsList = access.sqlqueryWithParams(query, connectionsMap);
            System.out.println("connectionsList::" + connectionsList);

        } catch (Exception e) {
        }
        return connectionsList;
    }

    @Transactional
    public String deleteErpConnectionDetails(HttpServletRequest request) {
        String result = "";
        try {
            String deleteQuery = "DELETE DAL_DM_SAVED_CONNECTIONS WHERE AUDIT_ID = :AUDIT_ID";
            System.out.println("query::" + deleteQuery);
            Map deleteConnectionsMap = new HashMap();
            deleteConnectionsMap.put("AUDIT_ID", request.getParameter("auditId"));

            int deleteCount = access.executeUpdateSQL(deleteQuery, deleteConnectionsMap);
            if (deleteCount != 0) {
                result = "Connection deleted successfully";
            }

        } catch (Exception e) {
            e.printStackTrace();
            result = "Failed to delete";
        }
        return result;
    }

    @Transactional
    public String updateErpConnectionDetails(HttpServletRequest request) {
        String result = "";
        try {
            String updateQuery = "UPDATE DAL_DM_SAVED_CONNECTIONS SET CONNECTION_NAME = :CONNECTION_NAME,HOST_NAME =:HOST_NAME,CONN_PORT = :CLIENT_ID,"
                    + "CONN_USER_NAME =:CONN_USER_NAME,CONN_PASSWORD = :CONN_PASSWORD,"
                    + "CONN_CUST_COL3 =:LANGUAGE_ID,CONN_DB_NAME =:CONN_DB_NAME,CONN_CUST_COL4 =:GROUP WHERE AUDIT_ID = :AUDIT_ID";
            System.out.println("query::" + updateQuery);
            Map updateConnectionsMap = new HashMap();
            updateConnectionsMap.put("CONNECTION_NAME", request.getParameter("connectionName"));
            updateConnectionsMap.put("HOST_NAME", request.getParameter("hostName"));
            updateConnectionsMap.put("CLIENT_ID", request.getParameter("ClientId"));
            updateConnectionsMap.put("CONN_USER_NAME", request.getParameter("userName"));
            updateConnectionsMap.put("CONN_PASSWORD", request.getParameter("password"));
            updateConnectionsMap.put("LANGUAGE_ID", request.getParameter("LanguageId"));
            updateConnectionsMap.put("CONN_DB_NAME", request.getParameter("ERPSystemId"));
            updateConnectionsMap.put("GROUP", request.getParameter("group")!=null?request.getParameter("group"):"");
            updateConnectionsMap.put("AUDIT_ID", request.getParameter("auditId"));
            int updateCount = access.executeUpdateSQL(updateQuery, updateConnectionsMap);
            if (updateCount != 0) {
                result = "Connection updated successfully";
            }

        } catch (Exception e) {
            e.printStackTrace();
            result = "Failed to update";
        }
        return result;
    }

    @Transactional
    public String saveErpConnectionDetails(HttpServletRequest request) {
        String result = "";
        try {
            DalDmSavedConnections dalDmSavedConnections = new DalDmSavedConnections();
            DalDmSavedConnectionsId savedConnectionsId = new DalDmSavedConnectionsId();
            savedConnectionsId.setConnectionName((request.getParameter("connectionName") != null && !"".equalsIgnoreCase(request.getParameter("connectionName"))) ? request.getParameter("connectionName") : "");
            savedConnectionsId.setOrgnId((String) request.getSession(false).getAttribute("ssOrgId"));
            savedConnectionsId.setUserName((String) request.getSession(false).getAttribute("ssUsername"));
            savedConnectionsId.setConnUserName((request.getParameter("userName") != null && !"".equalsIgnoreCase(request.getParameter("userName"))) ? request.getParameter("userName") : "");
            dalDmSavedConnections.setId(savedConnectionsId);
            dalDmSavedConnections.setHostName((request.getParameter("hostName") != null && !"".equalsIgnoreCase(request.getParameter("hostName"))) ? request.getParameter("hostName") : "");
            dalDmSavedConnections.setConnPort((request.getParameter("ClientId") != null && !"".equalsIgnoreCase(request.getParameter("ClientId"))) ? request.getParameter("ClientId") : "");
            dalDmSavedConnections.setConnPassword((request.getParameter("password") != null && !"".equalsIgnoreCase(request.getParameter("password"))) ? request.getParameter("password") : "");
            dalDmSavedConnections.setConnCustCol3((request.getParameter("LanguageId") != null && !"".equalsIgnoreCase(request.getParameter("LanguageId"))) ? request.getParameter("LanguageId") : "");
            dalDmSavedConnections.setConnDbName((request.getParameter("ERPSystemId") != null && !"".equalsIgnoreCase(request.getParameter("ERPSystemId"))) ? request.getParameter("ERPSystemId") : "");
            dalDmSavedConnections.setCreateBy(((String) request.getSession(false).getAttribute("ssUsername") != null && !"".equalsIgnoreCase((String) request.getSession(false).getAttribute("ssUsername"))) ? (String) request.getSession(false).getAttribute("ssUsername") : "");
            dalDmSavedConnections.setCreateDate(new Date());
            dalDmSavedConnections.setEditBy(((String) request.getSession(false).getAttribute("ssUsername") != null && !"".equalsIgnoreCase((String) request.getSession(false).getAttribute("ssUsername"))) ? (String) request.getSession(false).getAttribute("ssUsername") : "");
            dalDmSavedConnections.setEditDate(new Date());
            dalDmSavedConnections.setConnCustCol1((request.getParameter("selectedItemLabel") != null && !"".equalsIgnoreCase(request.getParameter("selectedItemLabel"))) ? request.getParameter("selectedItemLabel") : "");
            dalDmSavedConnections.setConnCustCol2((request.getParameter("ConnectionType") != null && !"".equalsIgnoreCase(request.getParameter("ConnectionType"))) ? request.getParameter("ConnectionType") : "");
            dalDmSavedConnections.setConnCustCol4((request.getParameter("group") != null && !"".equalsIgnoreCase(request.getParameter("group"))) ? request.getParameter("group") : "");
            dalDmSavedConnections.setAuditId(AuditIdGenerator.getAuditId("DAL_DM_SAVED_CONNECTIONS"));
            access.saveObj(dalDmSavedConnections);
            result = "true";
        } catch (Exception e) {

            e.printStackTrace();
            result = "false";
        }
        return result;
    }

    @Transactional
    public List getOracleErpSavedConnections(HttpServletRequest request) {
        List connectionsList = new ArrayList();
        try {
            String query = "SELECT CONNECTION_NAME,"//0
                    + " HOST_NAME,"//1
                    + "CONN_PORT, "//2
                    + "CONN_USER_NAME, "//3
                    + "CONN_PASSWORD,"//4
                    + "CONN_DB_NAME,"//5
                    + "CONN_CUST_COL1,"//6
                    + "AUDIT_ID "//8
                    + "FROM DAL_DM_SAVED_CONNECTIONS WHERE "
                    + "ORGN_ID = :ORGN_ID AND "
                    + "CONN_CUST_COL2 = :CONN_CUST_COL2"
                    + " AND USER_NAME =:USER_NAME";
            System.out.println("query::" + query);
            Map connectionsMap = new HashMap();
            connectionsMap.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            connectionsMap.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));
            //18-02
            connectionsMap.put("CONN_CUST_COL2", "Oracle_ERP");
            // connectionsMap.put("CONN_CUST_COL2", "Oracle_ERP");
            System.out.println("query::" + query);
            connectionsList = access.sqlqueryWithParams(query, connectionsMap);
            System.out.println("connectionsList::" + connectionsList);

        } catch (Exception e) {
        }
        return connectionsList;
    }

    @Transactional
    public List getConDetails(HttpServletRequest request, String connectonName) {

        List<Object[]> connectionDetailsList = new ArrayList<>();
        try {

            String query = "SELECT "
                    + "CONNECTION_NAME, "//0
                    + "HOST_NAME, " //1
                    + "CONN_USER_NAME, "//2
                    + "CONN_PASSWORD, "//3
                    + "CONN_PORT, "//4
                    + "CONN_DB_NAME,"//5
                    + "CONN_CUST_COL1, "//6
                    + "CONN_CUST_COL2, "//7
                    + "CONN_CUST_COL3, "//8
                    + "CONN_CUST_COL4, "//9
                    + "AUDIT_ID "//10
                    + "FROM DAL_DM_SAVED_CONNECTIONS WHERE CONNECTION_NAME=:CONNECTION_NAME "
                    + " AND ORGN_ID=:ORGN_ID "
                    + " AND USER_NAME =:USER_NAME ";

            System.out.println("query::" + query);
            Map connectionsMap = new HashMap();
            connectionsMap.put("CONNECTION_NAME", connectonName);
            connectionsMap.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            connectionsMap.put("USER_NAME", (String) request.getSession(false).getAttribute("ssUsername"));

            System.out.println("query::" + query);
            connectionDetailsList = access.sqlqueryWithParams(query, connectionsMap);
            System.out.println("connectionsList::" + connectionDetailsList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionDetailsList;
    }

    @Transactional
    public int deleteDalDmSavedFile(HttpServletRequest request, String fileName) {
        int deleteCount = 0;
        try {
            
            String deleteQuery = "DELETE DAL_DM_SAVED_FILES WHERE FILE_NAME = :FILE_NAME";
            System.out.println("query::" + deleteQuery);
            Map deleteConnectionsMap = new HashMap();
            deleteConnectionsMap.put("FILE_NAME", fileName);

            deleteCount = access.executeUpdateSQL(deleteQuery, deleteConnectionsMap);

        } catch (Exception e) {
            e.printStackTrace();

        }
        return deleteCount;
    }

    @Transactional
    public String getDimTableName(HttpServletRequest request, String factTable, String factColumn) {
        String dimTable = "";
        try {
            String query = "SELECT TABLE_NAME FROM ALL_CONSTRAINTS WHERE "
                    + "CONSTRAINT_NAME IN ( SELECT ALL_CONSTRAINTS.R_CONSTRAINT_NAME "
                    + "FROM ALL_CONS_COLUMNS, ALL_CONSTRAINTS "
                    + "WHERE ALL_CONS_COLUMNS.CONSTRAINT_NAME = ALL_CONSTRAINTS.CONSTRAINT_NAME  "
                    + "AND ALL_CONS_COLUMNS.TABLE_NAME = '" + factTable + "' "
                    + "AND ALL_CONS_COLUMNS.COLUMN_NAME IN ('" + factColumn + "') )";
            List<String> dimTableList = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
            if (dimTableList != null && !dimTableList.isEmpty()) {
                dimTable = dimTableList.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dimTable;
    }

    // ravi cube
    @Transactional
    public JSONObject getSavedDBData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
//         String query = "SELECT "
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

//    @Transactional
//    public JSONObject getCurrentDBTables(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//        JSONArray treeObjArray = new JSONArray();
//        try {
//            String fieldName = "";
//            String tableName = "";
//            String level = request.getParameter("level");
//            String filterOperator = request.getParameter("filterOperator");
//            String filterValue = request.getParameter("filterValue");
//            if (level != null && "0".equalsIgnoreCase(level)) {
//                String schemaObjectType = request.getParameter("schemaObjectType");
//                String start = request.getParameter("start");
//                String limit = request.getParameter("limit");
//                if (schemaObjectType != null && "TABLES".equalsIgnoreCase(schemaObjectType)) {
//                    fieldName = "TABLE_NAME";
//                    tableName = "USER_TABLES";
//                } else if (schemaObjectType != null && "VIEWS".equalsIgnoreCase(schemaObjectType)) {
//                    fieldName = "VIEW_NAME";
//                    tableName = "USER_VIEWS";
//                }
//                String query = "SELECT " + fieldName + " AS FIELD_NAME FROM " + tableName;
//                if (filterOperator != null && !"".equalsIgnoreCase(filterOperator)
//                        && filterValue != null && !"".equalsIgnoreCase(filterValue)) {
//                    query += " WHERE  " + fieldName + " " + filterOperator + " '" + filterValue + "' ";
//                }
//                query = query + " ORDER BY " + fieldName;
//                query = "SELECT FIELD_NAME FROM (" + query + ")  OFFSET " + start + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
//                System.out.println("query :: " + query);
//                List tablesList = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
//                for (int i = 0; i < tablesList.size(); i++) {
//                    JSONObject treeObj = new JSONObject();
//                    JSONArray subItemArray = new JSONArray();
//                    JSONObject subItem = new JSONObject();
//
//                    String table = (String) tablesList.get(i);
//                    subItem.put("label", table);
//                    subItem.put("value", table);
//                    subItemArray.add(subItem);
//
//                    treeObj.put("description", table);
//                    treeObj.put("items", subItemArray);
//                    treeObj.put("label", table);
//                    treeObj.put("value", table);
//                    treeObjArray.add(treeObj);
//                }
//            } else if (level != null && "1".equalsIgnoreCase(level)) {
//
//                String schemaObjectType = request.getParameter("schemaObjectType");
//
//                String query = "SELECT COLUMN_NAME FROM  USER_TAB_COLUMNS  WHERE TABLE_NAME LIKE '" + schemaObjectType + "'";
//                System.out.println("query :: " + query);
//                List tablesList = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
//                for (int i = 0; i < tablesList.size(); i++) {
//                    JSONObject treeObj = new JSONObject();
//                    String table = (String) tablesList.get(i);
//                    treeObj.put("description", table);
//                    treeObj.put("label", table);
//                    treeObj.put("value", table);
//                    treeObjArray.add(treeObj);
//                }
//            }
//
//            resultObj.put("treeObjArray", treeObjArray);
////         String query = "SELECT "
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return resultObj;
//    }

    @Transactional
    public JSONObject getTableLabels(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        JSONArray treeObjArray = new JSONArray();
        try {

            String level = request.getParameter("level");
            String tableName = request.getParameter("tableName");
            String gridId = request.getParameter("gridId");
            String roleId = (String) request.getSession(false).getAttribute("ssRole");
            Map map = new HashMap();
            String query = "SELECT COL_NAME, COL_LABEL FROM DAL_GRID_ROLE_COL_LINK WHERE"
                    + " GRID_ID=:GRID_ID "
                    + " AND GRID_REF_TABLE=:GRID_REF_TABLE"
                    + " AND ROLE_ID=:ROLE_ID AND (COL_EDT_TYPE IS NULL OR COL_EDT_TYPE=:COL_EDT_TYPE)";
            System.out.println("query :: " + query);
            map.put("GRID_ID", gridId);
            map.put("GRID_REF_TABLE", tableName);
            map.put("ROLE_ID", roleId);
            map.put("COL_EDT_TYPE", "DISP_ONLY");
            List<Object[]> labelsList = access.sqlqueryWithParams(query, map);
            for (int i = 0; i < labelsList.size(); i++) {
                JSONObject treeObj = new JSONObject();
                String column = (String) labelsList.get(i)[0];
                String columnLabel = (String) labelsList.get(i)[1];
                treeObj.put("description", columnLabel);
                treeObj.put("label", columnLabel);
                treeObj.put("value", column);
                treeObjArray.add(treeObj);
            }

            resultObj.put("treeObjArray", treeObjArray);
//         String query = "SELECT "
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
    public JSONObject getOnlineServicesHtml(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        StringBuilder htmlString = new StringBuilder();
        try {

            String mediaType = request.getParameter("mediaType");
            htmlString.append("<div id='mainFormOSDivId'>");
            if (mediaType != null && !"".equalsIgnoreCase(mediaType) && !"null".equalsIgnoreCase(mediaType)
                    && "LinkedIn".equalsIgnoreCase(mediaType)) {
                htmlString.append("<div class=\"linkedinMainDivClass\">\r\n"
                        + "        <form action=\"#\" id='linkedinFormId' method=\"post\">\r\n"
                        + "            <div class=\"form-row\">\r\n"
                        + "                <div class=\"form-group\">\r\n"
                        + "                    <label for=\"source_url\" attrkey='LINKEDIN_PAGE_URL'>LINKEDIN PAGE URL <span style=\"color:red\">*</span>\r\n"
                        + "</label>\r\n"
                        + "                    <input type=\"text\" id=\"source_url\" apiUrlKey='ETL_LINKEDIN_URL' name=\"source_url\">\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "                </div>\r\n"
                        + "            </div>\r\n"
                        + "            <div class=\"form-row\">\r\n"
                        + "                <div class=\"form-group\">\r\n"
                        + "                    <label for=\"post_company_name\" attrkey='POST_COMPANY_NAME'>POST COMPANY NAME <span style=\"color:red\">*</span></label>\r\n"
                        + "                    <input type=\"text\" id=\"post_company_name\" name=\"post_company_name\" >\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "                </div>\r\n"
                        + "                <div class=\"form-group\">\r\n"
                        + "                    <label for=\"candidate_name\" attrkey='CANDIDATE_NAME'>CANDIDATE NAME <span style=\"color:red\">*</span></label>\r\n"
                        + "                    <input type=\"text\" id=\"candidate_name\" name=\"candidate_name\" >\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "                </div>\r\n"
                        + "            </div>\r\n"
                        + "            <div class=\"form-row\">\r\n"
                        + "                <div class=\"form-group\">\r\n"
                        + "                    <label for=\"from_date\" attrkey='FROM_DATE'>FROM DATE</label>\r\n"
                        + "                    <input type=\"date\" id=\"from_date\" name=\"from_date\">\r\n"
                        + "                </div>\r\n"
                        + "                <div class=\"form-group\">\r\n"
                        + "                    <label for=\"to_date\" attrkey='TO_DATE'>TO_DATE</label>\r\n"
                        + "                    <input type=\"date\" id=\"to_date\" name=\"to_date\" >\r\n"
                        + "                </div>\r\n"
                        + "            </div>\r\n"
                        + "        </form>\r\n"
                        + "    </div>");
            } else if (mediaType != null && !"".equalsIgnoreCase(mediaType) && !"null".equalsIgnoreCase(mediaType)
                    && "Facebook".equalsIgnoreCase(mediaType)) {
                htmlString.append("<div class=\"facebookMainDivClass\">\r\n"
                        + "        <form action=\"#\" id='facebookFormId' method=\"post\">\r\n"
                        + "            <div class=\"form-group\">\r\n"
                        + "                <label for=\"page_url\" attrkey='FB_PAGE_URL'>FB PAGE URL <span style=\"color:red\">*</span></label>\r\n"
                        + "                <input type=\"text\" id=\"page_url\" apiUrlKey='ETL_FACEBOOK_URL' name=\"page_url\" required>\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "            </div>\r\n"
                        + "            <div class=\"form-row\">\r\n"
                        + "                <div class=\"form-group\">\r\n"
                        + "                    <label for=\"from_date\" attrkey='FROM_DATE'>FROM DATE <span style=\"color:red\">*</span></label>\r\n"
                        + "                    <input type=\"date\" id=\"from_date\" name=\"from_date\" required>\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "                </div>\r\n"
//                        + "                <div class=\"form-group\">\r\n"
//                        + "                    <label for=\"to_date\" attrkey='TO_DATE'>TO DATE <span style=\"color:red\">*</span></label>\r\n"
//                        + "                    <input type=\"date\" id=\"to_date\" name=\"to_date\" required>\r\n"
//                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
//                        + "                </div>\r\n"
                        + "            </div>\r\n"
                        + "</form></div>\r\n");

            } else if (mediaType != null && !"".equalsIgnoreCase(mediaType) && !"null".equalsIgnoreCase(mediaType)
                    && "Twitter".equalsIgnoreCase(mediaType)) {
                htmlString.append("<div class=\"facebookMainDivClass\">\r\n"
                        + "        <form action=\"#\" id='twitterFormId' method=\"post\">\r\n"
                        + "            <div class=\"form-group\">\r\n"
                        + "                <label for=\"page_url\" attrkey='HANDLE_NAME'>HANDLE NAME <span style=\"color:red\">*</span></label>\r\n"
                        + "                <input type=\"text\" id=\"page_url\" apiUrlKey='ETL_TWITTER_URL' name=\"page_url\" required>\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "            </div>\r\n"
                        + "            <div class=\"form-row\">\r\n"
                        + "                <div class=\"form-group\">\r\n"
                        + "                    <label for=\"from_date\" attrkey='FROM_DATE'>FROM DATE <span style=\"color:red\">*</span></label>\r\n"
                        + "                    <input type=\"date\" id=\"from_date\" name=\"from_date\" required>\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "                </div>\r\n"
//                        + "                <div class=\"form-group\">\r\n"
//                        + "                    <label for=\"to_date\">todate <span style=\"color:red\">*</span></label>\r\n"
//                        + "                    <input type=\"date\" id=\"to_date\" name=\"to_date\" required>\r\n"
//                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
//                        + "                </div>\r\n"
                        + "            </div>\r\n"
                        + "</form></div>\r\n");
            }
            else if (mediaType != null && !"".equalsIgnoreCase(mediaType) && !"null".equalsIgnoreCase(mediaType)
                    && "Youtube".equalsIgnoreCase(mediaType)) {
                htmlString.append("<div class=\"facebookMainDivClass\">\r\n"
                        + "        <form action=\"#\" id='twitterFormId' method=\"post\">\r\n"
                        + "            <div class=\"form-group\">\r\n"
                        + "                <label for=\"page_url\" attrkey='CHANNEL_URL'>CHANNEL URL <span style=\"color:red\">*</span></label>\r\n"
                        + "                <input type=\"text\" id=\"page_url\" apiUrlKey='ETL_YOUTUBE_URL' name=\"page_url\" required>\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "            </div>\r\n"
                        + "            <div class=\"form-row\">\r\n"
                        + "                <div class=\"form-group\">\r\n"
                        + "                    <label for=\"from_date\" attrkey='FROM_DATE'>FROM DATE <span style=\"color:red\">*</span></label>\r\n"
                        + "                    <input type=\"date\" id=\"from_date\" name=\"from_date\" required>\r\n"
                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
                        + "                </div>\r\n"
//                        + "                <div class=\"form-group\">\r\n"
//                        + "                    <label for=\"to_date\">TO DATE <span style=\"color:red\">*</span></label>\r\n"
//                        + "                    <input type=\"date\" id=\"to_date\" name=\"to_date\" required>\r\n"
//                        + "<div><span class='errorMessage' style='color:red; display:none'></span></div>"
//                        + "                </div>\r\n"
                        + "            </div>\r\n"
                        + "</form></div>\r\n");
            }
            htmlString.append("</div>");

            resultObj.put("result", htmlString);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
    @Transactional
    public JSONObject getDriveApiDetails(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

            String query = "SELECT "
                    + "KEYNAME, "//0
                    + "PROCESS_VALUE " //1
                    + "FROM B_APPL_PROPERTIES "
                    + "WHERE KEYNAME IN ('DRIVE_API_KEY', 'DRIVE_CLIENT_ID', 'DRIVE_APP_ID', 'DRIVE_DISCOVERY_DOC', 'DRIVE_SCOPES')";
            System.out.println(" query ::: " + query);
            List<Object[]> listResultObj = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
            if (listResultObj != null && !listResultObj.isEmpty()) {
                for (Object[] row : listResultObj) {
                    String keyName = (String) row[0];
                    String processValue = (String) row[1];
                    resultObj.put(keyName, processValue);
                }
            }
            //System.out.println(listResultObj);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;

    }
}