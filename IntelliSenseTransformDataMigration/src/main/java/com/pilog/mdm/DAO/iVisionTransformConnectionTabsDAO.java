/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;

import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.PilogUtilities;
import com.pilog.mdm.pojo.DalTreeParams;
import java.sql.PreparedStatement;



import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author Ravindar.P
 */
@Repository
public class iVisionTransformConnectionTabsDAO {
	
	
	
    @Autowired
    private DataAccess access;
    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;

    
    @Transactional
    public List getDataList(HttpServletRequest request, JSONObject infoObject) {
		List dataList = new ArrayList();
		try {
//            List columnsList = (List)infoObject.get("columnsList");
//            String tableName = (String)infoObject.get("tableName");
			String query = (String) infoObject.get("query");
			System.out.println("query :: " + query);
//            String columnsListStr = (String)columnsList.stream().map(e -> e).collect(Collectors.joining(", "));
			Map map = new HashMap();
//            String query = "SELECT "+columnsListStr+ " FROM "+ tableName;
			dataList = access.executeNativeSQL(query, map);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataList;
	}
    
    
    
    @Transactional
    public List<Object[]> getAvailableConnections(HttpServletRequest request, String parent) {
        List<Object[]> etlComponentsArray = new ArrayList<>();
        try {
            String selectQuery = "SELECT  "
                    + " SEQUENCE_NO,"//0
                    + " IMAGE_PATH,"//1
                    + " TYPE,"//2
                    + " DATA_FUN_NAME,"//3
                    + " TITLE,"//4
                    + " DESCRIPTION,"//5
                    + " DIV_ID,"//6
                    + " STYLE_ATTRIBUTE,"//7
                    + " PARENT, "//8
                    + " DIV_LEVEL "//9
                    + "  FROM   C_AVAILABLE_CONNECTIONS"
                    + " WHERE  "
                    + " PARENT=:PARENT "
                    + "  ORDER BY SEQUENCE_NO ";
            Map<String, Object> selectMap = new HashMap<>();

            selectMap.put("PARENT", parent);
            System.out.println("selectQuery::::" + selectQuery);
            etlComponentsArray = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return etlComponentsArray;
    }

    @Transactional
    public List getAvaliableSchemas(HttpServletRequest request, String schema, String type) {
        List etlSchemasArray = new ArrayList();
        try {
            String selectQuery = "SELECT  CONNECTION_NAME "
                    + "  FROM   DAL_DM_SAVED_CONNECTIONS "
                    + " WHERE CONN_CUST_COL1=:CONN_CUST_COL1 "
                    + " AND CONN_CUST_COL2=:CONN_CUST_COL2 AND ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME "
                    + "  ORDER BY CREATE_DATE ";
            Map<String, Object> selectMap = new HashMap<>();
            if (schema != null && !"".equalsIgnoreCase(schema) && !"null".equalsIgnoreCase(schema)) {
                selectMap.put("CONN_CUST_COL1", schema);
            } else {
                selectQuery = "SELECT  CONNECTION_NAME "
                        + "  FROM   DAL_DM_SAVED_CONNECTIONS "
                        + " WHERE CONN_CUST_COL1 IS NOT NULL "
                        + " AND CONN_CUST_COL2=:CONN_CUST_COL2 AND ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME "
                        + "  ORDER BY CREATE_DATE ";
            }
            selectMap.put("CONN_CUST_COL2", type);

            selectMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            selectMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            System.out.println("selectQuery::::" + selectQuery);
            etlSchemasArray = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return etlSchemasArray;
    }

    @Transactional
    public List<Object[]> getAvaliableFiles(HttpServletRequest request, String fileType) {
        List etlSchemasArray = new ArrayList();
        try {
            String selectQuery = "SELECT  FILE_ORG_NAME,FILE_NAME,FILE_PATH "
                    + "  FROM   DAL_DM_SAVED_FILES "
                    + " WHERE FILE_TYPE=:FILE_TYPE "
                    + " AND  ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME "
                    + "  ORDER BY CREATE_DATE ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("FILE_TYPE", fileType);
            selectMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            selectMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            System.out.println("selectQuery::::" + selectQuery);
            etlSchemasArray = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return etlSchemasArray;
    }

    public JSONArray fetchSchemaTables(HttpServletRequest request, String parentkeyData) {
        JSONArray treeDataObjArray = new JSONArray();
        try {
            int count = 0;
            int endTreeIndex = 0;
            JSONArray savedDbList = new JSONArray();
            String columnsObjStr = request.getParameter("columnsObj");
            String startIndex = request.getParameter("startIndex");
            String endIndex = request.getParameter("endIndex");
            if (columnsObjStr != null && !"".equalsIgnoreCase(columnsObjStr)) {
                JSONObject treeColumnsObj = (JSONObject) JSONValue.parse(columnsObjStr);
                if (treeColumnsObj != null && !treeColumnsObj.isEmpty()) {
                    String levelStr = request.getParameter("level");
                    String extTreeParams = request.getParameter("extTreeParams");
                    JSONObject columnsObj = (JSONObject) treeColumnsObj.get("4");
                    if (columnsObj != null && !columnsObj.isEmpty()) {
                        JSONObject treeInitParams = (JSONObject) columnsObj.get("TREE_INIT_PARAMS");
                        String showMoreIcon = "";
                        if (treeInitParams != null && !treeInitParams.isEmpty()) {
                            showMoreIcon = (String) treeInitParams.get("uuu_treeEtlShowMoreIcon");
                            String dataBaseConnectFlag = (String) treeInitParams.get("uuu_DataBaseConnectivityFlag");
//                            if (dataBaseConnectFlag != null && !"".equalsIgnoreCase(dataBaseConnectFlag) && "Y".equalsIgnoreCase(dataBaseConnectFlag)) {
                            savedDbList = getSavedConnections(request);
//                            }
                        }
                        List dataList = new ArrayList();
                        String connObj = request.getParameter("connectionObj");
                        JSONObject dataObj = new JSONObject();
                        if (connObj != null && !"".equalsIgnoreCase(connObj) && !"null".equalsIgnoreCase(connObj)) {
                            String filterValue = request.getParameter("filterValue");
                            JSONObject jsConnObj = (JSONObject) JSONValue.parse(connObj);
                            String connName = (String) jsConnObj.get("CONNECTION_NAME");
                            if (connName != null && !"".equalsIgnoreCase(connName) && "Current_V10".equalsIgnoreCase(connName)) {
                                dataObj = getTreeDataOpt(request, parentkeyData, columnsObj, levelStr);
                                if (dataObj != null && !dataObj.isEmpty()) {
                                    dataList = (List) dataObj.get("listData");
                                    count = (Integer) dataObj.get("countData");
                                }
                            } else if (filterValue != null && !"".equalsIgnoreCase(filterValue) && !"null".equalsIgnoreCase(filterValue)) {
                                dataObj = getTreeDataFilterOpt(request, parentkeyData, columnsObj, connObj, levelStr);
                                if (dataObj != null && !dataObj.isEmpty()) {
                                    dataList = (List) dataObj.get("listData");
                                    count = (Integer) dataObj.get("countData");
                                }
                            } else {
                                dataObj = getTreeDataOpt(request, parentkeyData, columnsObj, connObj, levelStr);
                                if (dataObj != null && !dataObj.isEmpty()) {
                                    dataList = (List) dataObj.get("listData");
                                    count = (Integer) dataObj.get("countData");
                                }
                            }
                        } else {
                            dataObj = getTreeDataOpt(request, parentkeyData, columnsObj, levelStr);
                            if (dataObj != null && !dataObj.isEmpty()) {
                                dataList = (List) dataObj.get("listData");
                                count = (Integer) dataObj.get("countData");
                            }
                        }
                        if (dataList != null && !dataList.isEmpty()) {
                            for (int i = 0; i < dataList.size(); i++) {
                                Object[] treeDataArray = (Object[]) dataList.get(i);
                                if (treeDataArray != null && treeDataArray.length != 0) {
                                    JSONObject treeObj = new JSONObject();
                                    treeObj.put("label", treeDataArray[1]);
                                    //treeObj.put("id", treeDataArray[0]);
                                    //treeObj.put("description", treeDataArray[1]);
                                    treeObj.put("description", treeDataArray[1]);
                                    JSONArray childArray = new JSONArray();
                                    JSONObject dummyObj = new JSONObject();
                                    dummyObj.put("value", "ajax");
                                    dummyObj.put("label", treeDataArray[0]);
                                    childArray.add(dummyObj);
                                    treeObj.put("items", childArray);
                                    treeObj.put("value", treeDataArray[0]);
                                    if (i == 0) {
                                        if (savedDbList != null && !savedDbList.isEmpty()) {
                                            JSONObject jsConnectionObj = new JSONObject();
                                            for (int k = 0; k < savedDbList.size(); k++) {
                                                JSONObject jsConnObj = (JSONObject) savedDbList.get(k);
                                                if (jsConnObj != null && !jsConnObj.isEmpty()) {
                                                    jsConnectionObj.put(jsConnObj.get("CONNECTION_NAME"), savedDbList.get(k));
                                                }
                                            }
                                            treeObj.put("connectionObj", jsConnectionObj);
                                        }
                                    }
                                    if (connObj != null && !"".equalsIgnoreCase(connObj) && !"null".equalsIgnoreCase(connObj)) {
                                        String connObject = "<input type='hidden' id='source_" + treeDataArray[1] + "' value='" + connObj + "'/>";
                                        treeObj.put("connObj", connObject);
                                    }

                                    treeDataObjArray.add(treeObj);

                                }

                            }
                        }
                        if (endIndex != null && !"".equalsIgnoreCase(endIndex) && !"null".equalsIgnoreCase(endIndex)) {
                            endTreeIndex = Integer.parseInt(endIndex);
                        }
                        if (levelStr != null && !"".equalsIgnoreCase(levelStr) && "4".equalsIgnoreCase(levelStr)
                                || (treeInitParams != null
                                && !treeInitParams.isEmpty()
                                && "Y".equalsIgnoreCase(String.valueOf(treeInitParams.get("uuu_enablePagination"))))) {
                            if (count > endTreeIndex) {
                                JSONObject treeObj = new JSONObject();
                                treeObj.put("label", "Show More...");
                                treeObj.put("description", "Show More...");
                                JSONArray childArray = new JSONArray();
                                JSONObject dummyObj = new JSONObject();
                                dummyObj.put("value", "ajax");
                                dummyObj.put("label", "Show More...");
                                childArray.add(dummyObj);
                                treeObj.put("items", childArray);
                                treeObj.put("value", "Show More");
                                if (showMoreIcon != null
                                        && !"".equalsIgnoreCase(showMoreIcon)
                                        && !"null".equalsIgnoreCase(showMoreIcon)) {
                                    treeObj.put("icon", showMoreIcon);
                                }

                                //treeObj.put("icon", "../images/vertical_dots.png");
                                treeDataObjArray.add(treeObj);
                            }
                        }
                        if (savedDbList != null && !savedDbList.isEmpty() && parentkeyData != null
                                && !"".equalsIgnoreCase(parentkeyData) && !"null".equalsIgnoreCase(parentkeyData)
                                && dbURL != null && !"".equalsIgnoreCase(dbURL)
                                && !"null".equalsIgnoreCase(dbURL) && dbURL.contains(parentkeyData.toLowerCase())) {
                            JSONObject treeObj = new JSONObject();
                            treeObj.put("label", "Current V10");
                            //treeObj.put("id", treeDataArray[0]);
                            treeObj.put("description", "Current_V10");
                            JSONArray childArray = new JSONArray();
                            JSONObject dummyObj = new JSONObject();
                            dummyObj.put("value", "ajax");
                            dummyObj.put("label", "Current V10");
                            childArray.add(dummyObj);
                            treeObj.put("items", childArray);
                            treeObj.put("value", "Current_V10");
                            treeDataObjArray.add(treeObj);
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return treeDataObjArray;
    }

    @Transactional
    public JSONObject getTreeDataOpt(HttpServletRequest request, String parentkeyData, JSONObject columnsObj, String connObj, String levelStr) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        JSONObject resultObj = new JSONObject();
        levelStr = "4";
        try {
            List treeList = new ArrayList();
            JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);
            JSONObject dbObj = new JSONObject();
            dbObj.put("selectedItemLabel", connectionObj.get("CONN_CUST_COL1"));
            dbObj.put("hostName", connectionObj.get("HOST_NAME"));
            dbObj.put("port", connectionObj.get("CONN_PORT"));
            dbObj.put("userName", connectionObj.get("CONN_USER_NAME"));
            dbObj.put("password", connectionObj.get("CONN_PASSWORD"));
            dbObj.put("serviceName", connectionObj.get("CONN_DB_NAME"));
            Connection ConnectionObj = (Connection) getConnection(dbObj);
            System.out.println("columnsObj:::" + columnsObj);
            if (levelStr != null && !"".equalsIgnoreCase(levelStr) && "3".equalsIgnoreCase(levelStr)) {
                parentkeyData = "%";
            }
            String treeId = request.getParameter("treeId");
            String startIndex = request.getParameter("startIndex");
            String endIndex = request.getParameter("endIndex");
            JSONObject treeInitParams = (JSONObject) columnsObj.get("TREE_INIT_PARAMS");
            String processClassAndMethod = "";
            String selectQuery = "";
            if (treeInitParams != null && !treeInitParams.isEmpty()) {
                processClassAndMethod = (String) treeInitParams.get("uuu_processClassAndMethod");
            }
            if (processClassAndMethod != null && !"".equalsIgnoreCase(processClassAndMethod) && !"null".equalsIgnoreCase(processClassAndMethod)) {
                String processArr[] = processClassAndMethod.split(",");
                if (processArr != null && processArr.length >= 2) {
                    String processClass = processArr[0];
                    String processMethod = processArr[1];
                    String selectedDbLabel = (String) connectionObj.get("CONN_CUST_COL1");
                    String serviceName = (String) connectionObj.get("CONN_DB_NAME");
                    String filterVal = "";
                    String filterCond = "";
                    if (processClass != null && !"".equalsIgnoreCase(processClass) && !"null".equalsIgnoreCase(processClass)
                            && processMethod != null && !"".equalsIgnoreCase(processMethod) && !"null".equalsIgnoreCase(processMethod)
                            && selectedDbLabel != null && !"".equalsIgnoreCase(selectedDbLabel) && !"Oracle".equalsIgnoreCase(selectedDbLabel)) {
                        try {
                            Class clazz = Class.forName(processClass);
                            Class<?>[] paramTypes = {HttpServletRequest.class, JSONObject.class,
                                String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class};
                            Object targetObj = new PilogUtilities().createObjectByName(processClass);
                            Method method = clazz.getMethod(processMethod.trim(), paramTypes);
                            selectQuery = (String) method.invoke(targetObj, request, columnsObj, parentkeyData,
                                    levelStr, startIndex, endIndex, serviceName, selectedDbLabel, filterVal, filterCond);
                            String countQuery = "SELECT  COUNT(*) FROM (" + selectQuery + ") AS COUNT";
                            System.out.println("Tree Data query::" + countQuery);
                            preparedStatement = ConnectionObj.prepareStatement(countQuery);
                            resultSet = preparedStatement.executeQuery();
                        } catch (ClassNotFoundException cnf) {
                            cnf.printStackTrace();
                        } catch (NoSuchMethodException nse) {
                            nse.printStackTrace();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        if (levelStr != null && !"".equalsIgnoreCase(levelStr) && !"null".equalsIgnoreCase(levelStr) && "4".equalsIgnoreCase(levelStr)) {
                            if (selectedDbLabel != null && !"".equalsIgnoreCase(selectedDbLabel) && "MYSQL".equalsIgnoreCase(selectedDbLabel)) {
                                selectQuery = selectQuery + " LIMIT " + startIndex + "," + endIndex + "";
                                selectQuery = "SELECT  * FROM (" + selectQuery + ") AS ABC";
                            } else if (selectedDbLabel != null && !"".equalsIgnoreCase(selectedDbLabel) && "SQLSERVER".equalsIgnoreCase(selectedDbLabel)) {
                                selectQuery = selectQuery + " ORDER BY FLD_NAME OFFSET " + startIndex + " ROWS FETCH NEXT " + endIndex + " ROWS ONLY";
                                selectQuery = "SELECT  * FROM (" + selectQuery + ") AS XYZ";
                            }
                        }
                    } else {
                        selectQuery = "SELECT  " + columnsObj.get("FLD_NAME") + " as FLD_NAME," + columnsObj.get("DISP_FLD_NAME") + " as DISP_FLD_NAME"
                                + " FROM " + columnsObj.get("TREE_REF_TABLE");
                        if (parentkeyData != null && !"".equalsIgnoreCase(parentkeyData) && !"null".equalsIgnoreCase(parentkeyData)) {
                            selectQuery += " WHERE " + columnsObj.get("HL_FLD_NAME") + " LIKE '" + parentkeyData + "'";
                        }
                        JSONObject initParam = (JSONObject) columnsObj.get("TREE_INIT_PARAMS");
                        if (initParam != null && !initParam.isEmpty()) {
                            if (initParam.get("uuu_SelectedMasterColumn") != null
                                    && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))
                                    && !"null".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))) {
                                String masterSelectedValue = request.getParameter("masterSelectedValue");
                                if (masterSelectedValue != null
                                        && !"".equalsIgnoreCase(masterSelectedValue)
                                        && !"null".equalsIgnoreCase(masterSelectedValue)) {
                                    if (selectQuery.contains(" WHERE ")) {
                                        selectQuery += " AND " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";

                                    } else {
                                        selectQuery += " WHERE " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";

                                    }
                                }
                            }

                        }
                        if (columnsObj.get("TREE_PARAMS_ID") != null
                                && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))
                                && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))) {
                            List paramsList = getTreeParamList((String) columnsObj.get("TREE_PARAMS_ID"), (String) request.getSession(false).getAttribute("ssOrgId"),
                                    (String) request.getSession(false).getAttribute("ssRole"));
                            if (paramsList != null && !paramsList.isEmpty()) {
                                String condition = getQueryFromTreeParams(paramsList, request);
                                if (condition != null && !"".equalsIgnoreCase(condition) && selectQuery.contains(" WHERE ")) {
                                    selectQuery += " AND " + condition;
                                } else if (condition != null && !"".equalsIgnoreCase(condition)) {
                                    selectQuery += " WHERE " + condition;
                                }
                            }
                        }

                        if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeOrderBy") != null
                                && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeOrderBy")))) {
                            selectQuery += " ORDER BY " + initParam.get("uuu_TreeOrderBy");
                        }
                        if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeGroupBy") != null
                                && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeGroupBy")))) {
                            selectQuery += " GROUP BY " + initParam.get("uuu_TreeGroupBy");
                        }

                        String countQuery = "SELECT  COUNT(*) FROM (" + selectQuery + ")";
                        if (levelStr != null && !"".equalsIgnoreCase(levelStr) && !"null".equalsIgnoreCase(levelStr) && "4".equalsIgnoreCase(levelStr)) {
                            selectQuery = "SELECT  * FROM (" + selectQuery + ") OFFSET " + startIndex + " ROWS FETCH NEXT " + endIndex + " ROWS ONLY";
                        }
                        System.out.println("Tree Data query::" + countQuery);
                        preparedStatement = ConnectionObj.prepareStatement(countQuery);
                        resultSet = preparedStatement.executeQuery();
                    }
                }
            } else {
                selectQuery = "SELECT  " + columnsObj.get("FLD_NAME") + " as FLD_NAME," + columnsObj.get("DISP_FLD_NAME") + " as DISP_FLD_NAME"
                        + " FROM " + columnsObj.get("TREE_REF_TABLE");
                if (parentkeyData != null && !"".equalsIgnoreCase(parentkeyData) && !"null".equalsIgnoreCase(parentkeyData)) {
                    selectQuery += " WHERE " + columnsObj.get("HL_FLD_NAME") + " LIKE '" + parentkeyData + "'";
                }
                JSONObject initParam = (JSONObject) columnsObj.get("TREE_INIT_PARAMS");
                if (initParam != null && !initParam.isEmpty()) {
                    if (initParam.get("uuu_SelectedMasterColumn") != null
                            && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))
                            && !"null".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))) {
                        String masterSelectedValue = request.getParameter("masterSelectedValue");
                        if (masterSelectedValue != null
                                && !"".equalsIgnoreCase(masterSelectedValue)
                                && !"null".equalsIgnoreCase(masterSelectedValue)) {
                            if (selectQuery.contains(" WHERE ")) {
                                selectQuery += " AND " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";

                            } else {
                                selectQuery += " WHERE " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";

                            }
                        }
                    }

                }
                if (columnsObj.get("TREE_PARAMS_ID") != null
                        && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))
                        && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))) {
                    List paramsList = getTreeParamList((String) columnsObj.get("TREE_PARAMS_ID"), (String) request.getSession(false).getAttribute("ssOrgId"),
                            (String) request.getSession(false).getAttribute("ssRole"));
                    if (paramsList != null && !paramsList.isEmpty()) {
                        String condition = getQueryFromTreeParams(paramsList, request);
                        if (condition != null && !"".equalsIgnoreCase(condition) && selectQuery.contains(" WHERE ")) {
                            selectQuery += " AND " + condition;
                        } else if (condition != null && !"".equalsIgnoreCase(condition)) {
                            selectQuery += " WHERE " + condition;
                        }
                    }
                }

                if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeOrderBy") != null
                        && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeOrderBy")))) {
                    selectQuery += " ORDER BY " + initParam.get("uuu_TreeOrderBy");
                }
                if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeGroupBy") != null
                        && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeGroupBy")))) {
                    selectQuery += " GROUP BY " + initParam.get("uuu_TreeGroupBy");
                }

                String countQuery = "SELECT  COUNT(*) FROM (" + selectQuery + ")";
                if (levelStr != null && !"".equalsIgnoreCase(levelStr) && !"null".equalsIgnoreCase(levelStr) && "4".equalsIgnoreCase(levelStr)) {
                    selectQuery = "SELECT  * FROM (" + selectQuery + ") OFFSET " + startIndex + " ROWS FETCH NEXT " + endIndex + " ROWS ONLY";
                }
                System.out.println("Tree Data query::" + countQuery);
                preparedStatement = ConnectionObj.prepareStatement(countQuery);
                resultSet = preparedStatement.executeQuery();
            }

            Integer count = 0;
            while (resultSet.next()) {
                count = resultSet.getInt(1);
            }
            resultSet.close();
            System.out.println("Tree Data query::" + selectQuery);
            preparedStatement = ConnectionObj.prepareStatement(selectQuery);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String objectType = resultSet.getString("FLD_NAME");
                String objectDesc = resultSet.getString("DISP_FLD_NAME");
                Object[] dataObj = new Object[2];
                dataObj[0] = objectType;
                dataObj[1] = objectDesc;
                treeList.add(dataObj);
            }
            resultObj.put("listData", treeList);
            resultObj.put("countData", count);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;

    }

    @Transactional
    public List getTreeParamList(String treeId, String orgnId, String role) {
        List paramsList = null;
        try {

            String gridParamQuery = "from DalTreeParams where id.treeId=:treeId  and id.roleId = :roleId ORDER BY id.colSeq";

            Map<String, Object> paramsMap = new HashMap<String, Object>();
            paramsMap.put("treeId", treeId.replaceAll("\\r|\\n", ""));
            // paramsMap.put("orgnId", orgnId.replaceAll("\\r|\\n", ""));
            paramsMap.put("roleId", role.replaceAll("\\r|\\n", ""));
            //System.out.println("gridParamQuery:::" + gridParamQuery);
            //System.out.println("paramsMap:::" + paramsMap);
            paramsList = access.queryWithParams(gridParamQuery, paramsMap);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return paramsList;
    }

    @Transactional
    public String getQueryFromTreeParams(List paramsList, HttpServletRequest request) {
        String whereCondition = "";
        try {
            if (paramsList != null && !paramsList.isEmpty()) {
                String extTreeParamsStr = request.getParameter("extTreeParams");
                JSONObject extTreeParams = new JSONObject();
                if (extTreeParamsStr != null
                        && !"".equalsIgnoreCase(extTreeParamsStr)
                        && !"null".equalsIgnoreCase(extTreeParamsStr)) {
                    extTreeParams = (JSONObject) JSONValue.parse(extTreeParamsStr);
                }
                for (int i = 0; i < paramsList.size(); i++) {

                    DalTreeParams dalGridParam = (DalTreeParams) paramsList.get(i);

                    String value = "";
                    if (dalGridParam.getColFlag() != null && dalGridParam.getColFlag().equalsIgnoreCase("S")) {
                        value = (String) request.getSession(false).getAttribute(dalGridParam.getColValue());
                    } else if (dalGridParam.getColFlag() != null && (dalGridParam.getColFlag().equalsIgnoreCase("F")
                            || dalGridParam.getColFlag().equalsIgnoreCase("A"))) {
                        value = dalGridParam.getColValue();
                    } else if (dalGridParam.getColFlag() != null && dalGridParam.getColFlag().equalsIgnoreCase("Q")) {
//                        value = visionGenericDAO.excuteParamQuery(dalGridParam.getColValue(), request);
                    } else if (dalGridParam.getColFlag() != null && dalGridParam.getColFlag().equalsIgnoreCase("R")) {
                        if (extTreeParams != null && !extTreeParams.isEmpty()) {
                            value = (String) extTreeParams.get(dalGridParam.getColValue());
                        }

                    }
                    // System.err.println("value::::"+value);
                    String operator = "";
                    if (i > 0) {

                        operator = dalGridParam.getAndOr();
                    }
                    if (dalGridParam.getOperator() != null && !"".equalsIgnoreCase(dalGridParam.getOperator())
                            && dalGridParam.getColFlag() != null && !dalGridParam.getColFlag().equalsIgnoreCase("A") && ("IN".equalsIgnoreCase(dalGridParam.getOperator()) || "NOT IN".equalsIgnoreCase(dalGridParam.getOperator()))) {

                        if (value != null && !"".equalsIgnoreCase(value)) {
                            String[] values = value.split(",");
                            value = "";
                            for (int j = 0; j < values.length; j++) {
                                value += "'" + values[j] + "'";
                                if (j != values.length - 1) {
                                    value += ",";

                                }
                            }
                            whereCondition += " " + operator + " " + dalGridParam.getId().getColName() + " " + dalGridParam.getOperator() + "(" + value + ")";
                        }

                    } else if (dalGridParam.getColFlag() != null && dalGridParam.getColFlag().equalsIgnoreCase("A")) {
                        if ((value.contains("<<--") && value.contains("-->>")) || (value.contains("<<-") && value.contains("->>"))) {
//                            value = visionGenericDAO.replaceSessionValues(value, request);
                        }
                        whereCondition += " " + operator + " " + dalGridParam.getId().getColName() + " " + dalGridParam.getOperator() + " " + value + "";
                    } else if (dalGridParam.getColFlag() != null && dalGridParam.getColFlag().equalsIgnoreCase("F") && ("IS".equalsIgnoreCase(dalGridParam.getOperator()) || "IS NOT".equalsIgnoreCase(dalGridParam.getOperator()))) {
                        whereCondition += " " + operator + " " + dalGridParam.getId().getColName() + " " + dalGridParam.getOperator() + " " + value + " ";
                    } else {
                        whereCondition += " " + operator + " " + dalGridParam.getId().getColName() + " " + dalGridParam.getOperator() + " '" + value + "' ";
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return whereCondition;
    }

    public Object getConnection(JSONObject dbObj) {

        Connection connection = null;
        Object returnedObj = null;
        try {

            String initParamClassName = "com.pilog.mdm.trasformaccess.V10MigrationDataAccess";
            String initParamMethodName = "get" + dbObj.get("selectedItemLabel") + "Connection";
            System.out.println(" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
            Class clazz = Class.forName(initParamClassName);
            Class<?>[] paramTypes = {String.class, String.class, String.class, String.class, String.class};
            Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
            Object targetObj = new PilogUtilities().createObjectByClass(clazz);
            returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("hostName")), String.valueOf(dbObj.get("port")),
                    String.valueOf(dbObj.get("userName")), String.valueOf(dbObj.get("password")), String.valueOf(dbObj.get("serviceName")));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnedObj;
    }

    @Transactional
    public JSONObject getTreeDataOpt(HttpServletRequest request, String parentkeyData, JSONObject columnsObj, String levelStr) {
        JSONObject treeDataObj = new JSONObject();
        try {
            List treeDataList = new ArrayList();
            System.out.println("columnsObj:::" + columnsObj);
            String distnictKeyword = " DISTINCT ";
            JSONObject initParam = (JSONObject) columnsObj.get("TREE_INIT_PARAMS");
            if (initParam != null && !initParam.isEmpty()
                    && initParam.get("uuu_dualQueryFlag") != null
                    && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_dualQueryFlag")))
                    && "Y".equalsIgnoreCase(String.valueOf(initParam.get("uuu_dualQueryFlag")))) {
                distnictKeyword = "";
            }
            int treeStartIndex = 0;
            int treeEndIndex = 0;
            System.out.println("columnsObj:::" + columnsObj);
            String treeId = request.getParameter("treeId");
            String startIndex = request.getParameter("startIndex");
            String endIndex = request.getParameter("endIndex");
            if (startIndex != null && !"".equalsIgnoreCase(startIndex) && !"null".equalsIgnoreCase(startIndex)
                    && endIndex != null && !"".equalsIgnoreCase(endIndex) && !"null".equalsIgnoreCase(endIndex)) {

                treeStartIndex = Integer.parseInt(startIndex);
                treeEndIndex = Integer.parseInt(endIndex);
                if (treeStartIndex != 0) {
                    treeStartIndex = treeStartIndex - 1;
                }
            }
            String selectQuery = "SELECT " + distnictKeyword + columnsObj.get("FLD_NAME") + " as FLD_NAME," + columnsObj.get("DISP_FLD_NAME") + " as DISP_FLD_NAME"
                    + " FROM " + columnsObj.get("TREE_REF_TABLE");

            if (parentkeyData != null && !"".equalsIgnoreCase(parentkeyData) && !"null".equalsIgnoreCase(parentkeyData)) {
                if ("Current_V10".equalsIgnoreCase(parentkeyData)) {
                    parentkeyData = "%";
                }
                if (initParam != null && !initParam.isEmpty()
                        && initParam.get("uuu_dualQueryLikeFlag") != null
                        && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_dualQueryLikeFlag")))
                        && "Y".equalsIgnoreCase(String.valueOf(initParam.get("uuu_dualQueryLikeFlag")))) {

                } else {
                    selectQuery += " WHERE " + columnsObj.get("HL_FLD_NAME") + " LIKE '" + parentkeyData + "'";
                }
            }
            String filterValue = request.getParameter("filterValue");
            if (filterValue != null && !"".equalsIgnoreCase(filterValue) && !"null".equalsIgnoreCase(filterValue)) {
                filterValue = filterValue.toUpperCase();
            }
            if (initParam != null && !initParam.isEmpty()) {
                if (initParam.get("uuu_selectedFilterColumn") != null
                        && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_selectedFilterColumn")))
                        && !"null".equalsIgnoreCase(String.valueOf(initParam.get("uuu_selectedFilterColumn")))) {
                    String filterQuery = "";
                    String columnName = String.valueOf(initParam.get("uuu_selectedFilterColumn"));
                    String filterCondition = request.getParameter("filterCondition");
                    if (filterCondition != null && !"".equalsIgnoreCase(filterCondition) && !"null".equalsIgnoreCase(filterCondition)) {
                        filterQuery = "  " + columnName + " " + filterCondition + " '" + filterValue + "' ";
                        if (selectQuery.contains(" WHERE ")) {
                            selectQuery += " AND " + filterQuery;
                        } else {
                            selectQuery = " WHERE " + filterQuery;
                        }
                    }
                }

                if (initParam.get("uuu_SelectedMasterColumn") != null
                        && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))
                        && !"null".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))) {
                    String masterSelectedValue = request.getParameter("masterSelectedValue");
                    if (masterSelectedValue != null
                            && !"".equalsIgnoreCase(masterSelectedValue)
                            && !"null".equalsIgnoreCase(masterSelectedValue)) {
                        if (selectQuery.contains(" WHERE ")) {
                            selectQuery += " AND " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";
                        } else {
                            selectQuery += " WHERE " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";
                        }
                    }
                }
            }
            if (columnsObj.get("TREE_PARAMS_ID") != null
                    && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))
                    && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))) {
                List paramsList = getTreeParamList((String) columnsObj.get("TREE_PARAMS_ID"), (String) request.getSession(false).getAttribute("ssOrgId"),
                        (String) request.getSession(false).getAttribute("ssRole"));
                if (paramsList != null && !paramsList.isEmpty()) {
                    String condition = getQueryFromTreeParams(paramsList, request);
                    if (condition != null && !"".equalsIgnoreCase(condition) && selectQuery.contains(" WHERE ")) {
                        selectQuery += " AND " + condition;
                    } else if (condition != null && !"".equalsIgnoreCase(condition)) {
                        selectQuery += " WHERE " + condition;
                    }
                }
            }

            if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeOrderBy") != null
                    && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeOrderBy")))) {
                selectQuery += " ORDER BY " + initParam.get("uuu_TreeOrderBy");
            }
            if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeGroupBy") != null
                    && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeGroupBy")))) {
                selectQuery += " GROUP BY " + initParam.get("uuu_TreeGroupBy");
            }

            String countQuery = "SELECT  COUNT(*) FROM (" + selectQuery + ")";
            System.out.println("Tree Data query::" + countQuery);
            List countList = access.sqlqueryWithParams(countQuery, Collections.EMPTY_MAP);
            if (countList != null && !countList.isEmpty()) {
                int count = new PilogUtilities().convertIntoInteger(countList.get(0));
                treeDataObj.put("countData", count);

            }
            List treeList = new ArrayList();
            System.out.println("Tree Data query::" + selectQuery);
            if ((levelStr != null
                    && !"".equalsIgnoreCase(levelStr)
                    && !"null".equalsIgnoreCase(levelStr)
                    && "4".equalsIgnoreCase(levelStr))
                    || (initParam != null
                    && !initParam.isEmpty()
                    && "Y".equalsIgnoreCase(String.valueOf(initParam.get("uuu_enablePagination"))))) {
                treeList = access.sqlqueryWithParamsLimit(selectQuery, Collections.EMPTY_MAP, (treeEndIndex - treeStartIndex), treeStartIndex);
            } else {
                treeList = access.sqlqueryWithParams(selectQuery, Collections.EMPTY_MAP);
            }
            treeDataObj.put("listData", treeList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return treeDataObj;

    }

    @Transactional
    public List<Object[]> getSelectedDbValues(HttpServletRequest request, String schemaName) {
        //     SELECT  DISTINCT OBJECT_TYPE as FLD_NAME,OBJECT_NAME as DISP_FLD_NAME FROM V_B_DB_OBJECTS_TYPS WHERE CONNECTION_NAME LIKE 'MYSQL_TEST' AND   ORGN_ID = 'C1F5CFB03F2E444DAE78ECCEAD80D27D'  AND USER_NAME = 'VBR_ETL'
        List etlSchemasArray = new ArrayList();
        try {
            String selectQuery = "SELECT  DISTINCT OBJECT_TYPE as FLD_NAME,OBJECT_NAME as DISP_FLD_NAME FROM V_B_DB_OBJECTS_TYPS "
                    + " WHERE CONNECTION_NAME=:CONNECTION_NAME "
                    + " AND  ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("CONNECTION_NAME", schemaName);
            selectMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            selectMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            System.out.println("selectQuery::::" + selectQuery);
            etlSchemasArray = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return etlSchemasArray;
    }

    @Transactional
    public String addSchemaToFav(HttpServletRequest request, String connectionName, String schemaType, String schema, String schemaPriority) {
        int updateCount = 0;
        String message = "";
        try {
            String updateQuery = "UPDATE DAL_DM_SAVED_CONNECTIONS SET CONN_CUST_COL10 =:CONN_CUST_COL10 WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND CONNECTION_NAME =:CONNECTION_NAME  AND CONN_CUST_COL1 =:CONN_CUST_COL1 AND CONN_CUST_COL2=:CONN_CUST_COL2 ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
            updateMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
            updateMap.put("CONN_CUST_COL10", schemaPriority);
            updateMap.put("CONNECTION_NAME", connectionName);
            updateMap.put("CONN_CUST_COL1", schema);
            updateMap.put("CONN_CUST_COL2", schemaType);
            System.out.println("updateQuery:::" + updateQuery);
            System.out.println("updateMap:::" + updateMap);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            if (updateCount > 0) {
                message = "Added to Favourite";
            } else {
                message = "Unable to Add";
            }
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    public List<Object[]> fetchFavSchemas(HttpServletRequest request, String schemaPriority) {
        List<Object[]> etlSchemasArray = new ArrayList();
        try {
            Map<String, Object> selectMap = new HashMap<>();
            String selectQuery = "SELECT  CONNECTION_NAME,CONN_CUST_COL1,CONN_CUST_COL2,CONN_CUST_COL10 "
                    + "  FROM   DAL_DM_SAVED_CONNECTIONS "
                    + " WHERE "
                    + "  CONN_CUST_COL10=:CONN_CUST_COL10 AND ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME "
                    + "  ORDER BY CREATE_DATE,CONN_CUST_COL2 ";
            selectMap.put("CONN_CUST_COL10", schemaPriority);
            selectMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            selectMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            System.out.println("selectQuery::::" + selectQuery);
            etlSchemasArray = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
        }
        return etlSchemasArray;
    }

    public String removeFavSchema(HttpServletRequest request, String connectionName, String schemaType) {
        int updateCount = 0;
        String message = "";
        try {
            String updateQuery = "UPDATE DAL_DM_SAVED_CONNECTIONS SET CONN_CUST_COL10 =:CONN_CUST_COL10 WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND CONNECTION_NAME =:CONNECTION_NAME  AND  CONN_CUST_COL2=:CONN_CUST_COL2 ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
            updateMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
            updateMap.put("CONN_CUST_COL10", "");
            updateMap.put("CONNECTION_NAME", connectionName);
            updateMap.put("CONN_CUST_COL2", schemaType);
            System.out.println("updateQuery:::" + updateQuery);
            System.out.println("updateMap:::" + updateMap);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            if (updateCount > 0) {
                message = "Favourite Schema Removed";
            } else {
                message = "Unable to Remove";
            }
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    public String addFilesToFav(HttpServletRequest request, String filePath, String fileName, String fileType, String filePriority) {
        int updateCount = 0;
        String message = "";
        try {
            String updateQuery = "UPDATE DAL_DM_SAVED_FILES SET FILE_CUST_COL10 =:FILE_CUST_COL10 WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND FILE_ORG_NAME =:FILE_ORG_NAME  AND  FILE_NAME=:FILE_NAME  AND  FILE_TYPE=:FILE_TYPE ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
            updateMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
            updateMap.put("FILE_CUST_COL10", filePriority);
            updateMap.put("FILE_ORG_NAME", fileName);
            updateMap.put("FILE_NAME", filePath);
            updateMap.put("FILE_TYPE", fileType);
            System.out.println("updateQuery:::" + updateQuery);
            System.out.println("updateMap:::" + updateMap);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            if (updateCount > 0) {
                message = "Added to Favourite";
            } else {
                message = "Unable to Add";
            }
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    public List<Object[]> fetchFavFiles(HttpServletRequest request, String filePriority) {
        List<Object[]> etlSchemasArray = new ArrayList();
        try {
            Map<String, Object> selectMap = new HashMap<>();
            String selectQuery = "SELECT  FILE_ORG_NAME,FILE_NAME,FILE_PATH,FILE_TYPE "
                    + "  FROM   DAL_DM_SAVED_FILES "
                    + " WHERE "
                    + "  FILE_CUST_COL10=:FILE_CUST_COL10 AND ORGN_ID=:ORGN_ID AND USER_NAME=:USER_NAME "
                    + "  ORDER BY CREATE_DATE ";
            selectMap.put("FILE_CUST_COL10", filePriority);
            selectMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            selectMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            System.out.println("selectQuery::::" + selectQuery);
            etlSchemasArray = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
        }
        return etlSchemasArray;
    }

    public String removeFavFile(HttpServletRequest request, String fileName, String fileOrgName) {
        int updateCount = 0;
        String message = "";
        try {
            String updateQuery = "UPDATE DAL_DM_SAVED_FILES SET FILE_CUST_COL10 =:FILE_CUST_COL10 WHERE ORGN_ID =:ORGN_ID AND "
                    + " USER_NAME =:USER_NAME AND FILE_NAME =:FILE_NAME  AND  FILE_ORG_NAME=:FILE_ORG_NAME ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));
            updateMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));
            updateMap.put("FILE_CUST_COL10", "");
            updateMap.put("FILE_NAME", fileOrgName);
            updateMap.put("FILE_ORG_NAME", fileName);
            System.out.println("updateQuery:::" + updateQuery);
            System.out.println("updateMap:::" + updateMap);
            updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
            if (updateCount > 0) {
                message = "Favourite File Removed";
            } else {
                message = "Unable to Remove";
            }
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    @Transactional
    public JSONArray getSavedConnections(HttpServletRequest request) {
        List connectionsList = new ArrayList();
        JSONArray savedDBArr = new JSONArray();
        try {
            String parentkeyData = request.getParameter("parentkey");
            String query = "SELECT CONNECTION_NAME,"//0
                    + " HOST_NAME,"//1
                    + "CONN_PORT, "//2
                    + "CONN_USER_NAME, "//3
                    + "CONN_PASSWORD,"//4
                    + "CONN_DB_NAME,"//5
                    + "CONN_CUST_COL1,"//6
                    + "CONN_CUST_COL4,"//7
                    + "AUDIT_ID "//8
                    + " FROM DAL_DM_SAVED_CONNECTIONS WHERE ORGN_ID = :ORGN_ID AND USER_NAME = :USER_NAME";
            System.out.println("query::" + query);
            Map connectionsMap = new HashMap();
            connectionsMap.put("ORGN_ID", "C1F5CFB03F2E444DAE78ECCEAD80D27D");
            //connectionsMap.put("ORGN_ID", (String) request.getSession(false).getAttribute("ssOrgId"));
            connectionsMap.put("USER_NAME", (String) request.getSession().getAttribute("ssUsername"));
            connectionsList = access.sqlqueryWithParams(query, connectionsMap);
            System.out.println("connectionsList::" + connectionsList);
            if (connectionsList != null && !connectionsList.isEmpty()) {
                for (int i = 0; i < connectionsList.size(); i++) {
                    Object connObj[] = (Object[]) connectionsList.get(i);
                    JSONObject connectionObj = new JSONObject();
                    connectionObj.put("CONNECTION_NAME", connObj[0]);
                    connectionObj.put("HOST_NAME", connObj[1]);
                    connectionObj.put("CONN_PORT", connObj[2]);
                    connectionObj.put("CONN_USER_NAME", connObj[3]);
                    connectionObj.put("CONN_PASSWORD", connObj[4]);
                    connectionObj.put("CONN_DB_NAME", connObj[5]);
                    connectionObj.put("CONN_CUST_COL1", connObj[6]);
                    if (String.valueOf(connObj[7])!=null && !"".equalsIgnoreCase(String.valueOf(connObj[7])) && !"null".equalsIgnoreCase(String.valueOf(connObj[7]))){
                    connectionObj.put("GROUP", connObj[7]);
                    }
                    
                    savedDBArr.add(connectionObj);
                }

                JSONObject dbDetailsObj = getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10");
                savedDBArr.add(dbDetailsObj);
            }
        } catch (Exception e) {
        }
        return savedDBArr;
    }

    @Transactional
    public JSONObject getDatabaseDetails(String dbDriver, String dbURL, String userName, String password, String connName) {
        JSONObject dbDetailsObj = new JSONObject();
        if (dbDriver != null && !"".equalsIgnoreCase(dbDriver) && !"null".equalsIgnoreCase(dbDriver)) {
            String hostName = "";
            String portNumber = "";
            String dbName = "";
            if (dbDriver.contains("oracle")) {
                String[] dbUrls = dbURL.split("/");
                if (dbUrls != null) {
                    String hostAndPort = dbUrls[2];
                    String[] hostPortDetails = hostAndPort.split(":");
                    if (hostPortDetails != null && hostPortDetails.length > 1) {
                        hostName = hostPortDetails[0];
                        portNumber = hostPortDetails[1];
                    }
                    dbName = dbUrls[3];
                    dbDetailsObj.put("CONN_CUST_COL1", "Oracle");
                }

            } else if (dbDriver.contains("sqlserver")) {

                String[] dbUrls = dbURL.split("/");
                if (dbUrls != null) {
                    String hostAndPort = dbUrls[2];
                    String[] hostPortDetails = hostAndPort.split(";");
                    if (hostPortDetails != null && hostPortDetails.length > 3) {
                        String hostAndPortValue = hostPortDetails[0];
                        String dbNameValue = hostPortDetails[1];

                        if (hostAndPortValue != null && !"".equalsIgnoreCase(hostAndPortValue) && !"".equalsIgnoreCase(hostAndPortValue)) {
                            String hostAndPortValues[] = hostAndPortValue.split(":");
                            if (hostAndPortValues != null) {
                                hostName = hostAndPortValues[0];
                                portNumber = hostAndPortValues[1];
                            }
                        }
                        if (dbNameValue != null && !"".equalsIgnoreCase(dbNameValue) && !"".equalsIgnoreCase(dbNameValue)) {
                            String dbNameValues[] = dbNameValue.split("=");
                            if (dbNameValues != null) {
                                dbName = dbNameValues[1];
                            }
                        }
                    }
                    dbDetailsObj.put("CONN_CUST_COL1", "SQLSERVER");
                }
            } else if (dbDriver.contains("mysql")) {
                String[] dbUrls = dbURL.split("/");
                if (dbUrls != null) {
                    String hostAndPort = dbUrls[2];
                    String[] hostPortDetails = hostAndPort.split(":");
                    if (hostPortDetails != null && hostPortDetails.length > 1) {
                        hostName = hostPortDetails[0];
                        portNumber = hostPortDetails[1];
                    }
                    dbName = dbUrls[3];
                    dbDetailsObj.put("CONN_CUST_COL1", "MYSQL");
                }
            }
            dbDetailsObj.put("CONNECTION_NAME", connName);
            dbDetailsObj.put("HOST_NAME", hostName);
            dbDetailsObj.put("CONN_PORT", portNumber);
            dbDetailsObj.put("CONN_USER_NAME", userName);
            dbDetailsObj.put("CONN_PASSWORD", password);
            dbDetailsObj.put("CONN_DB_NAME", dbName);

        }
        return dbDetailsObj;
    }

    @Transactional
    public JSONObject getTreeDataFilterOpt(HttpServletRequest request, String parentkeyData,
            JSONObject columnsObj, String connObj,
            String levelStr
    ) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        JSONObject resultObj = new JSONObject();
        try {
            List treeList = new ArrayList();
            JSONObject connectionObj = (JSONObject) JSONValue.parse(connObj);
            JSONObject dbObj = new JSONObject();
            dbObj.put("selectedItemLabel", connectionObj.get("CONN_CUST_COL1"));
            dbObj.put("hostName", connectionObj.get("HOST_NAME"));
            dbObj.put("port", connectionObj.get("CONN_PORT"));
            dbObj.put("userName", connectionObj.get("CONN_USER_NAME"));
            dbObj.put("password", connectionObj.get("CONN_PASSWORD"));
            dbObj.put("serviceName", connectionObj.get("CONN_DB_NAME"));
            Connection ConnectionObj = (Connection) getConnection(dbObj);
            System.out.println("columnsObj:::" + columnsObj);
            JSONObject initParam = (JSONObject) columnsObj.get("TREE_INIT_PARAMS");
            if (levelStr != null && !"".equalsIgnoreCase(levelStr) && "3".equalsIgnoreCase(levelStr)) {
                parentkeyData = "%";
            }
            String treeId = request.getParameter("treeId");
            String startIndex = request.getParameter("startIndex");
            String endIndex = request.getParameter("endIndex");
            String selectQuery = "";
            String processClassAndMethod = "";
            if (initParam != null && !initParam.isEmpty()) {
                processClassAndMethod = (String) initParam.get("uuu_processClassAndMethod");
            }
            if (processClassAndMethod != null && !"".equalsIgnoreCase(processClassAndMethod) && !"null".equalsIgnoreCase(processClassAndMethod)) {
                String processArr[] = processClassAndMethod.split(",");
                if (processArr != null && processArr.length >= 2) {
                    String processClass = processArr[0];
                    String processMethod = processArr[1];
                    String selectedDbLabel = (String) connectionObj.get("CONN_CUST_COL1");
                    String serviceName = (String) connectionObj.get("CONN_DB_NAME");
                    String filterValue = request.getParameter("filterValue");
                    String filterCond = request.getParameter("filterCondition");
                    if (filterValue != null && !"".equalsIgnoreCase(filterValue) && !"".equalsIgnoreCase(filterValue)) {
                        filterValue = filterValue.toUpperCase();
                    }
                    if (processClass != null && !"".equalsIgnoreCase(processClass) && !"null".equalsIgnoreCase(processClass)
                            && processMethod != null && !"".equalsIgnoreCase(processMethod) && !"null".equalsIgnoreCase(processMethod)
                            && selectedDbLabel != null && !"".equalsIgnoreCase(selectedDbLabel) && !"Oracle".equalsIgnoreCase(selectedDbLabel)) {
                        try {
                            Class clazz = Class.forName(processClass);
                            Class<?>[] paramTypes = {HttpServletRequest.class, JSONObject.class,
                                String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class};
                            Object targetObj = new PilogUtilities().createObjectByName(processClass);
                            Method method = clazz.getMethod(processMethod.trim(), paramTypes);
                            selectQuery = (String) method.invoke(targetObj, request, columnsObj, parentkeyData,
                                    levelStr, startIndex, endIndex, serviceName, selectedDbLabel, filterValue, filterCond);
                            String countQuery = "SELECT  COUNT(*) FROM (" + selectQuery + ") AS COUNT";
                            System.out.println("Tree Data query::" + countQuery);
                            preparedStatement = ConnectionObj.prepareStatement(countQuery);
                            resultSet = preparedStatement.executeQuery();
                        } catch (ClassNotFoundException cnf) {
                            cnf.printStackTrace();
                        } catch (NoSuchMethodException nse) {
                            nse.printStackTrace();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        if (levelStr != null && !"".equalsIgnoreCase(levelStr) && !"null".equalsIgnoreCase(levelStr) && "4".equalsIgnoreCase(levelStr)) {
                            if (selectedDbLabel != null && !"".equalsIgnoreCase(selectedDbLabel) && "MYSQL".equalsIgnoreCase(selectedDbLabel)) {
                                selectQuery = selectQuery + " LIMIT " + startIndex + "," + endIndex + "";
                                selectQuery = "SELECT  * FROM (" + selectQuery + ") AS ABC";
                            } else if (selectedDbLabel != null && !"".equalsIgnoreCase(selectedDbLabel) && "SQLSERVER".equalsIgnoreCase(selectedDbLabel)) {
                                selectQuery = selectQuery + " ORDER BY TABLE_NAME OFFSET " + startIndex + " ROWS FETCH NEXT " + endIndex + " ROWS ONLY";
                                selectQuery = "SELECT  * FROM (" + selectQuery + ") AS XYZ";
                            }
                        }
                    } else {
                        selectQuery = "SELECT " + columnsObj.get("FLD_NAME") + " as FLD_NAME," + columnsObj.get("DISP_FLD_NAME") + " as DISP_FLD_NAME "
                                + " FROM " + columnsObj.get("TREE_REF_TABLE");
                        if (filterValue != null && !"".equalsIgnoreCase(filterValue) && !"null".equalsIgnoreCase(filterValue)) {
                            filterValue = filterValue.toUpperCase();
                        }
                        if (parentkeyData != null && !"".equalsIgnoreCase(parentkeyData) && !"null".equalsIgnoreCase(parentkeyData)) {
                            selectQuery += " WHERE " + columnsObj.get("HL_FLD_NAME") + " LIKE '" + parentkeyData + "'";
                        }

                        if (initParam != null && !initParam.isEmpty()) {
                            if (initParam.get("uuu_selectedFilterColumn") != null
                                    && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_selectedFilterColumn")))
                                    && !"null".equalsIgnoreCase(String.valueOf(initParam.get("uuu_selectedFilterColumn")))) {
                                String filterQuery = "";
                                String columnName = String.valueOf(initParam.get("uuu_selectedFilterColumn"));
                                String filterCondition = request.getParameter("filterCondition");
                                if (filterCondition != null && !"".equalsIgnoreCase(filterCondition) && !"null".equalsIgnoreCase(filterCondition)) {

                                    filterQuery = "  " + columnName + " " + filterCondition + " '" + filterValue + "' ";
                                    if (selectQuery.contains(" WHERE ")) {
                                        selectQuery += " AND " + filterQuery;
                                    } else {
                                        selectQuery = " WHERE " + filterQuery;
                                    }
                                }
                            }

                            if (initParam.get("uuu_SelectedMasterColumn") != null
                                    && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))
                                    && !"null".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))) {
                                String masterSelectedValue = request.getParameter("masterSelectedValue");
                                if (masterSelectedValue != null
                                        && !"".equalsIgnoreCase(masterSelectedValue)
                                        && !"null".equalsIgnoreCase(masterSelectedValue)) {
                                    if (selectQuery.contains(" WHERE ")) {
                                        selectQuery += " AND " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";

                                    } else {
                                        selectQuery += " WHERE " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";

                                    }
                                }
                            }
                        }
                        if (columnsObj.get("TREE_PARAMS_ID") != null
                                && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))
                                && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))) {
                            List paramsList = getTreeParamList((String) columnsObj.get("TREE_PARAMS_ID"), (String) request.getSession(false).getAttribute("ssOrgId"),
                                    (String) request.getSession(false).getAttribute("ssRole"));
                            if (paramsList != null && !paramsList.isEmpty()) {
                                String condition = getQueryFromTreeParams(paramsList, request);
                                if (condition != null && !"".equalsIgnoreCase(condition) && selectQuery.contains(" WHERE ")) {
                                    selectQuery += " AND " + condition;
                                } else if (condition != null && !"".equalsIgnoreCase(condition)) {
                                    selectQuery += " WHERE " + condition;
                                }
                            }
                        }

                        if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeOrderBy") != null
                                && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeOrderBy")))) {
                            selectQuery += " ORDER BY " + initParam.get("uuu_TreeOrderBy");
                        }
                        if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeGroupBy") != null
                                && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeGroupBy")))) {
                            selectQuery += " GROUP BY " + initParam.get("uuu_TreeGroupBy");
                        }

                        String countQuery = "SELECT  COUNT(*) FROM (" + selectQuery + ")";
                        selectQuery = "SELECT  * FROM (" + selectQuery + ") OFFSET " + startIndex + " ROWS FETCH NEXT " + endIndex + " ROWS ONLY";

                        System.out.println("Tree Data query::" + countQuery);
                        preparedStatement = ConnectionObj.prepareStatement(countQuery);
                        resultSet = preparedStatement.executeQuery();
                    }
                }
            } else {
                selectQuery = "SELECT " + columnsObj.get("FLD_NAME") + " as FLD_NAME," + columnsObj.get("DISP_FLD_NAME") + " as DISP_FLD_NAME "
                        + " FROM " + columnsObj.get("TREE_REF_TABLE");
                String filterValue = request.getParameter("filterValue");
                if (filterValue != null && !"".equalsIgnoreCase(filterValue) && !"null".equalsIgnoreCase(filterValue)) {
                    filterValue = filterValue.toUpperCase();
                }
                if (parentkeyData != null && !"".equalsIgnoreCase(parentkeyData) && !"null".equalsIgnoreCase(parentkeyData)) {
                    selectQuery += " WHERE " + columnsObj.get("HL_FLD_NAME") + " LIKE '" + parentkeyData + "'";
                }

                if (initParam != null && !initParam.isEmpty()) {
                    if (initParam.get("uuu_selectedFilterColumn") != null
                            && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_selectedFilterColumn")))
                            && !"null".equalsIgnoreCase(String.valueOf(initParam.get("uuu_selectedFilterColumn")))) {
                        String filterQuery = "";
                        String columnName = String.valueOf(initParam.get("uuu_selectedFilterColumn"));
                        String filterCondition = request.getParameter("filterCondition");
                        if (filterCondition != null && !"".equalsIgnoreCase(filterCondition) && !"null".equalsIgnoreCase(filterCondition)) {

                            filterQuery = "  " + columnName + " " + filterCondition + " '" + filterValue + "' ";
                            if (selectQuery.contains(" WHERE ")) {
                                selectQuery += " AND " + filterQuery;
                            } else {
                                selectQuery = " WHERE " + filterQuery;
                            }
                        }
                    }

                    if (initParam.get("uuu_SelectedMasterColumn") != null
                            && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))
                            && !"null".equalsIgnoreCase(String.valueOf(initParam.get("uuu_SelectedMasterColumn")))) {
                        String masterSelectedValue = request.getParameter("masterSelectedValue");
                        if (masterSelectedValue != null
                                && !"".equalsIgnoreCase(masterSelectedValue)
                                && !"null".equalsIgnoreCase(masterSelectedValue)) {
                            if (selectQuery.contains(" WHERE ")) {
                                selectQuery += " AND " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";

                            } else {
                                selectQuery += " WHERE " + initParam.get("uuu_SelectedMasterColumn") + " LIKE '" + masterSelectedValue + "' ";

                            }
                        }
                    }
                }
                if (columnsObj.get("TREE_PARAMS_ID") != null
                        && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))
                        && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("TREE_PARAMS_ID")))) {
                    List paramsList = getTreeParamList((String) columnsObj.get("TREE_PARAMS_ID"), (String) request.getSession(false).getAttribute("ssOrgId"),
                            (String) request.getSession(false).getAttribute("ssRole"));
                    if (paramsList != null && !paramsList.isEmpty()) {
                        String condition = getQueryFromTreeParams(paramsList, request);
                        if (condition != null && !"".equalsIgnoreCase(condition) && selectQuery.contains(" WHERE ")) {
                            selectQuery += " AND " + condition;
                        } else if (condition != null && !"".equalsIgnoreCase(condition)) {
                            selectQuery += " WHERE " + condition;
                        }
                    }
                }

                if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeOrderBy") != null
                        && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeOrderBy")))) {
                    selectQuery += " ORDER BY " + initParam.get("uuu_TreeOrderBy");
                }
                if (initParam != null && !initParam.isEmpty() && initParam.get("uuu_TreeGroupBy") != null
                        && !"".equalsIgnoreCase(String.valueOf(initParam.get("uuu_TreeGroupBy")))) {
                    selectQuery += " GROUP BY " + initParam.get("uuu_TreeGroupBy");
                }

                String countQuery = "SELECT  COUNT(*) FROM (" + selectQuery + ")";
                selectQuery = "SELECT  * FROM (" + selectQuery + ") OFFSET " + startIndex + " ROWS FETCH NEXT " + endIndex + " ROWS ONLY";

                System.out.println("Tree Data query::" + countQuery);
                preparedStatement = ConnectionObj.prepareStatement(countQuery);
                resultSet = preparedStatement.executeQuery();

            }
            Integer count = 0;
            while (resultSet.next()) {
                count = resultSet.getInt(1);
            }
            resultSet.close();

            System.out.println("Tree Data query::" + selectQuery);
            preparedStatement = ConnectionObj.prepareStatement(selectQuery);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String objectType = resultSet.getString("FLD_NAME");
                String objectDesc = resultSet.getString("DISP_FLD_NAME");
                Object[] dataObj = new Object[2];
                dataObj[0] = objectType;
                dataObj[1] = objectDesc;
                treeList.add(dataObj);
            }
            resultObj.put("listData", treeList);
            resultObj.put("countData", count);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;

    }
    
    @Transactional
    public List getErpTypes(HttpServletRequest request) {
        List getErpTypesArr = new ArrayList();
        try {
            String selectQuery = " SELECT CONN_CUST_COL1 "
                    + " FROM DAL_DM_SAVED_CONNECTIONS WHERE CONN_CUST_COL1 in('SAP','Oracle_ERP')"
                    + " AND USER_NAME=:USER_NAME "
                    + " AND ORGN_ID=:ORGN_ID "
                    + " GROUP BY CONN_CUST_COL1 ";
            Map<String, Object> selectMap = new HashMap<>();
            selectMap.put("ORGN_ID", request.getSession(false).getAttribute("ssOrgId"));//ORGN_ID
            selectMap.put("USER_NAME", request.getSession(false).getAttribute("ssUsername"));//USER_NAME
            System.out.println("selectQuery::::" + selectQuery);
            getErpTypesArr = access.sqlqueryWithParams(selectQuery, selectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return getErpTypesArr;
    }


}
