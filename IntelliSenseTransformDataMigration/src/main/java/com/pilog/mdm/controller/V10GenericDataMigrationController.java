/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;


import com.pilog.mdm.service.V10GenericDataMigrationService;
import java.io.File;
import java.io.OutputStream;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

/**
 *
 * @author PiLog
 */
@Controller
public class V10GenericDataMigrationController {
	private String etlFilePath;
	{
		if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
			etlFilePath = "C://";
		} else {
			etlFilePath = "/u01/";
		}
	}

    @Autowired
    private V10GenericDataMigrationService v10GenericDataMigrationService;

    // need to check
    @RequestMapping(value = "/getDataMigrationMenus", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String getDataMigrationMenus(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            String menuResult = "";
            String gridId = request.getParameter("gridId");
            JSONArray treeMenuArray = v10GenericDataMigrationService.getTreeMenu(request, "DM_SOURCES");
//            System.out.println("treeMenuArray::::"+treeMenuArray);
            if (treeMenuArray != null && !treeMenuArray.isEmpty()) {
                menuResult = treeMenuArray.toJSONString();
            }
            String tabsDiv = "<div id='dataMigrationTabs' class='dataMigrationTabs'><ul class='dataMigrationTabsHeader'>"
                    + "<input type='hidden' id='gridIdValue' value='" + gridId + " '>"
                    + "<input type='hidden' id='filePathValue' value=''>"
                    + "<li class='dataMigrationTabsli'><a href='#tabs-1'>Migration Sources</a></li>"
                    + "<li class='dataMigrationTabsli' onclick='savedConnection()'><a href='#tabs-2'>Saved Connections</a></li>"
                    + "</ul>"
                    + "<div id='tabs-1' class='dataMigrationsTabsInner'>"
                    + " </div>"
                    + "<div id='tabs-2' class='dataMigrationsTabsInner'><div style='text-align:center;margin-top:15px' class='savedErrorMsg'>No saved connections found</div>"
                    + "<div id='sub-tabs-2'><ul class='dataMigrationTabsHeader'><li  onclick='savedConnection()'><a href = '#tab2-1'>Database Connections</a> </li>"
                    + "<li onclick='showErpSavedConnection()'><a href = '#tab2-2' >SAP Conections</a> </li> <li onclick='showOracleErpSavedConnection()'><a href = '#tab2-3' >Oracle ERP Conections</a> </li></ul></div>"
                    + "<div id = 'tab2-1'><div class='savedConnections' id='savedConnectionTable'></div></div> "
                    + "<div id = 'tab2-2'><div id='ERPSavedConnectionTable'></div></div><div id = 'tab2-3'><div id='OracleERPSavedConnectionTable'></div></div>"
                    + "</div></div>";

            String treeDiv = " <div class='dataMigratorTreeDiv' id='dataMigratorTreeDiv'>"
                    + "<div id='dataMigratorTree' class='dataMigratorTreeInner'>"
                    + "</div>"
                    + "</div>";
            String contentDiv = "<div class = 'dataMigratorContent'> "
                    + "<div class='visionProgressSteps'><ul class=\"visionStepsProgressbar\">\n"
                    + "<li id ='connectToDB' class=\"active\"><p class='visionProcessStepsContent'>Connect to Database</p></li>\n"
                    + "<li id='fetchTables'><p class='visionProcessStepsContent'>Fetch Tables</p> </li>\n"
                    + "<li id ='mapColumns'><p class='visionProcessStepsContent'>Map Columns</p></li>\n"
                    + "</ul></div>"
                    + "<div class='visionProgressFilesSteps'><ul class=\"visionStepsProgressbar\">\n"
                    + "<li id ='uploadFile' class=\"active\"><p class='visionProcessStepsContent'>Upload File</p></li>\n"
                    + "<li id='mapFileCols'><p class='visionProcessStepsContent'>Map Columns</p> </li>\n"
                    + "</ul></div>"
                    + "<div class='visionUploadFileDiv' id='visionUploadDocs'>"
                    + "<div id='showFileType' class='visionShowFileType'></div>"
                    + "<input type='hidden' id='selectedType' value=''>"
                    + "<input type='hidden' id='selectedTypeName' value=''>"
                    + "<input type='hidden' id='savedConnectionList' value=''>"
                    + "<input type='hidden' value='' id='dbDetails'>"
                    + "<input type='hidden' value='' id='auditId'>"
                    + "<div id = 'visionDMFileUploadDiv' class='visionDMFileUploadDiv'>"
                    + "<input type='file' name='importDMFile'  id='importDMFile' class='visionDMFilesInput'>"
                    + "<div class='visionDMFileUploadclass' id='visionDmFileUpload'>"
                    + "<p class='VisionDMUploadFileContent'>Upload Files Here </p>"
                    + "</div> <div id='visionDMFileList'></div>"
                    + "<div class='allErrors visionDMFiles' splitcount='0' id='disvisionDMFiles' style='color: red;display:none' ></div>"
                    + "</div>"
                    + "<div id='visionFileMapCols'></div>"
                    + "</div>"
                    + "<div class='visionDatabaseMain'>"
                    + "<div id='showConnectionType' class='visionShowConnectionType'></div>"
                    + "<div class='visionConnectToDbDiv' id='visionConnectToDb'>"
                    + "<table class='visionDbTable'>"
                    + " <tr class='visionDbTr'>"
                    + "<td class='visionDbTd'> <label class='visionDbLabels'>Connection Name</label></td>"
                    + "<td class='visionDbTd'> <input type='text' value='' name='ConnectionName' id='DbConnectionName' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='DbConnectionNameError'></div></td>"
                    + " </tr>"
                    + "<tr class='visionDbTr'>"
                    + "<td class='visionDbTd'> <label class='visionDbLabels'>Host Name</label></td>"
                    + "<td class='visionDbTd'> <input type='text' value='' name='HostName' id='DbHostName' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='DbHostNameError'></div></td>"
                    + "</tr>"
                    + "<tr class='visionDbTr'>"
                    + "<td class='visionDbTd'>  <label class='visionDbLabels'>Port</label></td>"
                    + "<td class='visionDbTd'><input type='text' value='' name='Port' id='DbPort' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='DbPortError'></div></td>"
                    + "</tr>"
                    + "<tr class='visionDbTr'>"
                    + "<td class='visionDbTd'>  <label class='visionDbLabels'>Username</label></td>"
                    + "<td class='visionDbTd'> <input type='text' value='' name='Username' id='DbUserName' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='DbUserNameError'></div></td>"
                    + "</tr>"
                    + " <tr class='visionDbTr'>"
                    + "<td class='visionDbTd'>  <label class='visionDbLabels'>Password</label></td>"
                    + "<td class='visionDbTd'>    <input type='password' value='' name='HostName' id='DbPassword' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='DbPasswordError'></div></td>"
                    + "</tr>"
                    + "<tr class='visionDbTr'>"
                    + "<td class='visionDbTd'>  <label class='visionDbLabels'>Database/Service Name</label></td>"
                    + "<td class='visionDbTd'>    <input type='text' value='' name='ServiceName' id='DbServiceName' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='DbServiceNameError'></div></td>"
                    + "</tr>"
                    + "<tr class='visionDbTr' style='display:none'>"
                    + "<td class='visionDbTd'>  <label class='visionDbLabels'>Audit Id</label></td>"
                    + "<td class='visionDbTd'>    <input type='hidden' value='' name='auditId' id='auditId' class='visionInputDbFields'></td>"
                    + "</tr>"
                    + "<tr class='visionDbTr'><td><input type='checkbox' name='checkBoxDetails' id = 'checkBoxChecked' value='checked' checked>Save Details"
                    + "<div class='visionDataMigrationError' style='display:none'>Please check the box</div></td></tr>"
                    + "<tr class='visionDbTr'>"
                    + "<td class='visionDbTd visionDbConnectBtn' id='connectDbTd' colspan = '2'><input type='button' value='Connect' name='Connect'  onclick = 'connectDatabase()' class='visionInputDbButton'></td>"
                    + "<td class='visionDbTd visionDbSaveBtn' id='saveDbTd' colspan = '2' style='display:none'></td>"
                    + "</tr>"
                    + "</table>"
                    + "</div>"
                    + "<div id='fieldChooser' class='visionTablesComboBox' style='display:none'></div>"
                    + "<div id='mappingColumns' class='visionColumnsMapping' style='display:none'>"
                    //            +    "<div id= 'showConnectionName' style='margin-left:15px;margin-top:15px'>  </div>"
                    + "<div id='showSourceTablesList' class='visionSourceTablesMain'></div>"
                    + "<div class='visionMappedTable' id= 'MappedTable'></div>"
                    + "<div class='visionProcessCols'>"
                    + "<input type='button' value='Process' name='Process' id='processCols' onclick = 'fetchSelectedColumns()' class='visionProcessColsBtn'>"
                    + "</div>"
                    + "</div>"
                    + "</div>"
                    + "</div>"
                    + "<div class='visionERPMain' id='visionERPMain' style='display:none'>"
                    + "<div id='showERPConnectionType' class='visionShowERPConnectionType'></div>"
                    + "<div class='visionERPInner'>"
                    + "<div class='visionERPTableDiv'>"
                    + "<table class='visionERPTable'>"
                    + " <tr class='visionERPDbTr'>"
                    + "<td class='visionERPDbTd'> <label class='visionERPDbLabels'>Connection Name</label></td>"
                    + "<td class='visionERPDbTd'> <input type='text' value='' name='ConnectionName' id='ErpDbConnectionName' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='ErpDbConnectionNameError'></div></td>"
                    + " </tr>"
                    + " <tr class='visionERPDbTr'>"
                    + "<td class='visionERPDbTd'> <label class='visionERPDbLabels'>Client</label></td>"
                    + "<td class='visionERPDbTd'> <input type='text' value='' name='Client' id='ERPClientName' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='ERPClientNameError'></div></td>"
                    + " </tr>"
                    + "<tr class='visionERPDbTr'>"
                    + "<td class='visionERPDbTd'> <label class='visionERPDbLabels'>Host Name</label></td>"
                    + "<td class='visionERPDbTd'> <input type='text' value='' name='ERP HostName' id='ERPHostName' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='ERPHostNameError'></div></td>"
                    + "</tr>"
                    + "<tr class='visionERPDbTr'>"
                    + "<td class='visionERPDbTd'>  <label class='visionERPDbLabels'>Username</label></td>"
                    + "<td class='visionERPDbTd'> <input type='text' value='' name='Username' id='ERPUserName' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='ERPUserNameError'></div></td>"
                    + "</tr>"
                    + " <tr class='visionERPDbTr'>"
                    + "<td class='visionERPDbTd'>  <label class='visionERPDbLabels'>Password</label></td>"
                    + "<td class='visionERPDbTd'>    <input type='password' value='' name='Password' id='ERPPassword' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='ERPPasswordError'></div></td>"
                    + "</tr>"
                    + "<tr class='visionERPDbTr'>"
                    + "<td class='visionERPDbTd'>  <label class='visionERPDbLabels'>Language Id</label></td>"
                    + "<td class='visionERPDbTd'>    <input type='text' value='' name='languageId' id='ERPLanguageId' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='ERPLanguageIdError'></div></td>"
                    + "</tr>"
                    + "<tr class='visionERPDbTr'>"
                    + "<tr class='visionERPDbTr' style='display:none'>"
                    + "<td class='visionERPDbTr'>  <label class='visionERPDbLabels'>Audit Id</label></td>"
                    + "<td class='visionERPDbTr'>    <input type='hidden' value='' name='auditId' id='auditId' class='visionInputDbFields'></td>"
                    + "</tr>"
                    + "<tr class='visionERPDbTr'>"
                    + "<td class='visionERPDbTd'>  <label class='visionERPDbLabels'>System Id</label></td>"
                    + "<td class='visionERPDbTd'>    <input type='text' value='' name='ERPSystemId' id='ERPSystemId' class='visionInputDbFields'>"
                    + "<div class='dataMigrationInputError' id='ERPSystemIdError'></div></td>"
                    + "</tr>"
                    + "<tr class='visionERPDbTr'><td><input type='checkbox' name='checkBoxDetails' id = 'checkBoxChecked' value='checked' checked>Save Details"
                    + "<div class='visionDataMigrationError' style='display:none'>Please check the box</div></td></tr>"
                    + "<tr class='visionERPDbTr'>"
                    + "<td class='visionERPDbTd visionERPDbConnectBtn' id='connectERPDbTd' colspan = '2'><input type='button' value='Connect' name='Connect'  onclick = 'connectErpDatabase()' class='visionInputDbButton'></td>"
                    + "<td class='visionERPDbTd visionERPDbSaveBtn' id='saveERPDbTd' colspan = '2' style='display:none'></td>"
                    + "</tr>"
                    + "</table>"
                    + "</div>"
                    + "</div>"
                    //                    + "<div class='visionMappedTable' id= 'MappedErpTable'></div>"
                    //                    + "<div class='visionProcessCols'>"
                    //                    + "<input type='button' value='Process' name='Process' id='processErpCols' onclick = 'fetchErpSelectedColumns()' class='visionProcessColsBtn'>"
                    //                    + "</div>"
                    + "</div>"
                    + "</div>";
            resultObj.put("menuResult", menuResult);
            resultObj.put("tabsDiv", tabsDiv);
            resultObj.put("treeDiv", treeDiv);
            resultObj.put("contentDiv", contentDiv);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj.toJSONString();
    }

    // method to view connection details in ETL under connections tree
    @RequestMapping(value = "/getConnectionDetails", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String getConnectionDetails(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();

        try {

            resultObj = v10GenericDataMigrationService.getConnectionDetails(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj.toJSONString();
    }

    // method to show saved connections under connections tree in ETL
    @RequestMapping(value = "/getSavedConnections", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String getSavedConnections(HttpServletRequest request) {

        String result = "<div class='savedConnectionsTable'><table style='width: 100%;' border='1'>"
                + "<tr><th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Connection Type</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Connection Name</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Host Name</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Port</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Username</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Password</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Database Name</th>"
                + "<th style='display:none;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Audit Id</th>"
                + "</tr>";

        try {
            List<Object[]> connectionsList = v10GenericDataMigrationService.getSavedConnections(request);
            if (connectionsList != null && !connectionsList.isEmpty()) {
                for (Object[] connectionArray : connectionsList) {
//                    System.out.println("connectionArray:::" + connectionArray);

                    result += "<tr style='cursor: pointer' ondblclick = connectDatabaseProcess('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','false','" + connectionArray[6] + "','" + connectionArray[7] + "')>"
                            + "<td><img src=\"images/change_requests_icon_2.png\" style='width:16px;height:16px;padding:2px' id=\"connectionEdit\" alt=\"Edit Connection\" onclick = editDBConnection('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','true','" + connectionArray[6] + "','" + connectionArray[7] + "') class='visionDBConnectIcon'>"
                            + "<img src=\"images/delete_icon1.png\"  id=\"connectionDelete\" style='padding:2px' alt=\"Delete Connection\" onclick = deleteDBConnection('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','false','" + connectionArray[6] + "','" + connectionArray[7] + "') class='visionDBConnectIcon'></td>"
                            + "<td>" + connectionArray[6] + "</td>"
                            + "<td>" + connectionArray[0] + "</td>"
                            + "<td>" + connectionArray[1] + "</td>"
                            + "<td>" + connectionArray[2] + "</td><td>" + connectionArray[3] + "</td>"
                            + "<td><input type='password' value = " + connectionArray[4] + " style='border: 0;' readonly></td><td>" + connectionArray[5] + " </td>"
                            + "<td style='display:none'>" + connectionArray[7] + "</td>"
                            + "</tr>";

                }
                result += "</table></div>";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @RequestMapping(value = "/fetchTableColumns", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject fetchTableColumns(HttpServletRequest request) {

        JSONObject columnsObject = new JSONObject();
        try {
            columnsObject = v10GenericDataMigrationService.fetchTableColumns(request);
        } catch (Exception e) {
        }
        return columnsObject;
    }

    @RequestMapping(value = "/mappingSourceColsWithDestCols", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String mappingSourceColsWithDestCols(HttpServletRequest request) {

        JSONObject columnsObject = new JSONObject();
        try {
            String connectionname = request.getParameter("selectedItemLabel");
            if (connectionname != null && "SAP ECC".equalsIgnoreCase(connectionname)) {
                columnsObject = v10GenericDataMigrationService.mappingErpSourceColsWithDestCols(request);
            } else {
                columnsObject = v10GenericDataMigrationService.mappingSourceColsWithDestCols(request);
            }
        } catch (Exception e) {
        }
        return columnsObject.toJSONString();
    }
//    @RequestMapping(value = "/mappingSourceColsWithDestCols", method = {RequestMethod.GET, RequestMethod.POST})
//    public @ResponseBody
//    String mappingSourceColsWithDestCols(HttpServletRequest request) {
//
//        JSONObject columnsObject = new JSONObject();
//        try {
//            columnsObject = v10GenericDataMigrationService.mappingSourceColsWithDestCols(request);
//        } catch (Exception e) {
//        }
//        return columnsObject.toJSONString();
//    }

    @RequestMapping(value = "/updateConnectionDetails", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String updateConnectionDetails(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = v10GenericDataMigrationService.getConnectionDetails(request);
        } catch (Exception e) {
        }
        return resultObj.toJSONString();
    }

//    @RequestMapping(value = "/deleteDatabaseDetails", method = {RequestMethod.GET, RequestMethod.POST})
//    public @ResponseBody
//    String deleteDatabaseDetails(HttpServletRequest request) {
//
//        String result = "";
//        try {
//            result = v10GenericDataMigrationService.deleteConnectionDetails(request);
//        } catch (Exception e) {
//        }
//        return result;
//    }
    @RequestMapping(value = "/deleteDatabaseDetails", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject deleteDatabaseDetails(HttpServletRequest request) { // ravi updated code changes

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = v10GenericDataMigrationService.deleteConnectionDetails(request);
        } catch (Exception e) {
        }
        return resultObj;
    }

    @RequestMapping(value = "/importDMFile", produces = "text/plain;charset=UTF-8")
    public @ResponseBody
    String importDMFile(HttpServletRequest request, HttpServletResponse response,
            @RequestParam("gridId") String gridId,
            @RequestParam("tableName") String tableName,
            @RequestParam("selectedFiletype") String selectedFiletype,
            @RequestParam("dataFeilds") String dataFeilds,
            @RequestParam("importDMFile") MultipartFile file) {
        System.out.println("Entered Export Controller...");
        String result = "";
        try {

            tableName = (tableName == null) ? "" : tableName;
            result = v10GenericDataMigrationService.importDMFile(request, response, gridId, file, tableName, selectedFiletype, dataFeilds);

        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.out.println("result::::" + result);
        return result;
    }

//    @RequestMapping(value = "/mappingSourceFileColsWithDestCols", method = {RequestMethod.GET, RequestMethod.POST})
//    public @ResponseBody
//    String mappingSourceFileColsWithDestCols(HttpServletRequest request, HttpServletResponse response) {
//
//        String result = "";
//        try {
//            result = v10GenericDataMigrationService.mappingSourceFileColsWithDestCols(request, response);
//        } catch (Exception e) {
//        }
//        return result;
//    }
    @RequestMapping(value = "/getErpConnectionDetails", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String getErpConnectionDetails(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();

        try {

            resultObj = v10GenericDataMigrationService.getErpConnectionDetails(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj.toJSONString();
    }

    @RequestMapping(value = "/getSavedErpConnections", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String getSavedErpConnections(HttpServletRequest request) {

        String result = "<div class='savedErpConnectionsTable'><table style='width: 100%;' border='1'>"
                + "<tr><th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Connection Type</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Connection Name</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Host Name</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>ClientId</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Username</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Password</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>ERPSystemId</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>LanguageId</th>"
                + "<th style='display:none;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Audit Id</th>"
                + "</tr>";

        try {
            List<Object[]> connectionsList = v10GenericDataMigrationService.getSavedErpConnections(request);
            if (connectionsList != null && !connectionsList.isEmpty()) {
                for (Object[] connectionArray : connectionsList) {
//                    System.out.println("connectionArray:::" + connectionArray);

                    result += "<tr style='cursor: pointer' ondblclick = connectErpDatabaseProcess('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','false','" + connectionArray[6] + "','" + connectionArray[7] + "','" + connectionArray[8] + "')>"
                            + "<td><img src=\"images/change_requests_icon_2.png\" style='width:16px;height:16px;padding:2px' id=\"connectionEdit\" alt=\"Edit Connection\" onclick = editErpConnection('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','true','" + connectionArray[6] + "','" + connectionArray[7] + "','" + connectionArray[8] + "') class='visionDBConnectIcon'>"
                            + "<img src=\"images/delete_icon1.png\"  id=\"connectionDelete\" style='padding:2px' alt=\"Delete Connection\" onclick = deleteERPConnection('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','false','" + connectionArray[6] + "','" + connectionArray[7] + "','" + connectionArray[8] + "') class='visionDBConnectIcon'></td>"
                            + "<td>" + connectionArray[6] + "</td>"
                            + "<td>" + connectionArray[0] + "</td>"
                            + "<td>" + connectionArray[1] + "</td>"
                            + "<td>" + connectionArray[2] + "</td><td>" + connectionArray[3] + "</td>"
                            + "<td><input type='password' value = " + connectionArray[4] + " style='border: 0;' readonly></td><td>" + connectionArray[5] + " </td>"
                            + "<td style='display:none'>" + connectionArray[8] + "</td>"
                            + "<td >" + connectionArray[7] + "</td>"
                            + "</tr>";

                }
                result += "</table></div>";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @RequestMapping(value = "/fetchErpTableColumns", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject fetchErpTableColumns(HttpServletRequest request) {

        JSONObject columnsObject = new JSONObject();
        try {
            columnsObject = v10GenericDataMigrationService.fetchErpTableColumns(request);
        } catch (Exception e) {
        }
        return columnsObject;
    }

    @RequestMapping(value = "/getOracleErpSavedConnections", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String getOracleErpSavedConnections(HttpServletRequest request) {

        String result = "<div class='savedConnectionsTable'><table style='width: 100%;' border='1'>"
                + "<tr><th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'></th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Connection Type</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Connection Name</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Host Name</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Port</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Username</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Password</th>"
                + "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Database Name</th>"
                + "<th style='display:none;background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Audit Id</th>"
                + "</tr>";

        try {
            List<Object[]> connectionsList = v10GenericDataMigrationService.getOracleErpSavedConnections(request);
            if (connectionsList != null && !connectionsList.isEmpty()) {
                for (Object[] connectionArray : connectionsList) {
//                    System.out.println("connectionArray:::" + connectionArray);

                    result += "<tr style='cursor: pointer' ondblclick = connectDatabaseProcess('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','false','" + connectionArray[6] + "','" + connectionArray[7] + "')>"
                            + "<td><img src=\"images/change_requests_icon_2.png\" style='width:16px;height:16px;padding:2px' id=\"connectionEdit\" alt=\"Edit Connection\" onclick = editDBConnection('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','true','" + connectionArray[6] + "','" + connectionArray[7] + "') class='visionDBConnectIcon'>"
                            + "<img src=\"images/delete_icon1.png\"  id=\"connectionDelete\" style='padding:2px' alt=\"Delete Connection\" onclick = deleteDBConnection('" + connectionArray[0] + "',"
                            + "'" + connectionArray[1] + "',"
                            + "'" + connectionArray[2] + "','" + connectionArray[3] + "',"
                            + "'" + connectionArray[4] + "','" + connectionArray[5] + "','false','" + connectionArray[6] + "','" + connectionArray[7] + "') class='visionDBConnectIcon'></td>"
                            + "<td>" + connectionArray[6] + "</td>"
                            + "<td>" + connectionArray[0] + "</td>"
                            + "<td>" + connectionArray[1] + "</td>"
                            + "<td>" + connectionArray[2] + "</td><td>" + connectionArray[3] + "</td>"
                            + "<td><input type='password' value = " + connectionArray[4] + " style='border: 0;' readonly></td><td>" + connectionArray[5] + " </td>"
                            + "<td style='display:none'>" + connectionArray[7] + "</td>"
                            + "</tr>";

                }
                result += "</table></div>";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @RequestMapping(value = "/deleteErpDetails", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String deleteErpDetails(HttpServletRequest request) {

        String result = "";
        try {
            result = v10GenericDataMigrationService.deleteErpConnectionDetails(request);
        } catch (Exception e) {
        }
        return result;
    }

    @RequestMapping(value = "/getConnDetails", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String getConnDetails(HttpServletRequest request) {

        String result = "";
        try {
            result = v10GenericDataMigrationService.getConnDetails(request);
        } catch (Exception e) {
        }
        return result;
    }

    @RequestMapping(value = "/updateConnDetails", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String updateConnDetails(HttpServletRequest request) {

        String result = "";
        try {
            result = v10GenericDataMigrationService.updateConnDetails(request);
        } catch (Exception e) {
        }
        return result;
    }

    @RequestMapping(value = "/deleteFile", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String deleteFile(HttpServletRequest request) {

        String result = "";
        try {
            String originalFileName = request.getParameter("fileName");
            String filePath = request.getParameter("filePath");
            String userName = (String) request.getSession(false).getAttribute("ssUsername");
            if (filePath != null && filePath.contains("\\") || filePath.contains("/")) {

            } else {
                filePath = etlFilePath+"Files\\TreeDMImport\\" + userName + "\\" + filePath;
            }

            File fileObj = new File(filePath);

//            File fileObj = new ClassPathResource("C:\\Files\\TreeDMImport\\KESHAV_MGR\\SPIRUploadSheet1613541228894.xls").getFile();
            if (fileObj.exists()) {
                if (fileObj.delete()) {
                    String fileName = fileObj.getName();
                    int deleteCount = v10GenericDataMigrationService.deleteDalDmSavedFile(request, fileName);
                    if (deleteCount > 0) {
                        result = "" + originalFileName + " Deleted Succesfully";
                    }

                } else {

                }
            } else {
                result = originalFileName + " does not exsit.";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

     @RequestMapping(value = "/deleteSelectedConnExplorerItems", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String deleteSelectedConnExplorerItems(HttpServletRequest request) {

        String result = "";
        int totalDeleteCount = 0;
        try {
            String fileConnObjArrayStr = request.getParameter("fileConnObjArray");
            JSONArray fileConnObjArray = (JSONArray)JSONValue.parse(fileConnObjArrayStr);
            for (Object fileConnObj : fileConnObjArray){
                JSONObject fileConnObject = (JSONObject)fileConnObj;
                String originalFileName = (String)fileConnObject.get("fileName");
                String filePath =  (String)fileConnObject.get("filePath");
                String userName = (String) request.getSession(false).getAttribute("ssUsername");
                if (filePath != null && filePath.contains("\\") || filePath.contains("/")) {

                } else {
                    filePath = etlFilePath+"Files\\TreeDMImport\\" + userName + "\\" + filePath;
                }

                File fileObj = new File(filePath);

    //            File fileObj = new ClassPathResource("C:\\Files\\TreeDMImport\\KESHAV_MGR\\SPIRUploadSheet1613541228894.xls").getFile();
                if (fileObj.exists()) {
                    if (fileObj.delete()) {
                        String fileName = fileObj.getName();
                        int deleteCount = v10GenericDataMigrationService.deleteDalDmSavedFile(request, fileName);
                        if (deleteCount > 0) {
                            totalDeleteCount += deleteCount;
                        }

                    } else {

                    }
                }
            }
             

        } catch (Exception e) {
            e.printStackTrace();
        }
        result = totalDeleteCount+" files deleted ";
        return result;
    }

    @RequestMapping(value = "/getSavedDBData", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject getSavedDBData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

            resultObj = v10GenericDataMigrationService.getSavedDBData(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = {"/readPDF"})
    public @ResponseBody
    void readPDF(HttpServletRequest request, HttpServletResponse response) {
        try {

//            v10GenericDataMigrationService.readPDF(request, response);
            String fileName = request.getParameter("filePath");
            String filePath = "";
            String username = (String) request.getSession(false).getAttribute("ssUsername");
            if (fileName.contains("---")) {
                filePath = fileName.replaceAll("---", "/");
            } else {
                filePath = etlFilePath+"Files/TreeDMImport" + File.separator + username + File.separator + fileName;
            }

            File file = new File(filePath);

            response.setContentType("application/pdf");
            response.setHeader("Content-Disposition", "inline; filename=" + fileName);

            byte[] data = FileUtils.readFileToByteArray(file);

            if (data == null) {
                data = "THIS IS AN EMPTY FILE!!".getBytes();
            }
            OutputStream o = response.getOutputStream();
            o.write(data);
            o.flush();
            o.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   
    
//    @RequestMapping(value = {"/getCurrentDBTables"})
//    public @ResponseBody
//    JSONObject getCurrentDBTables(HttpServletRequest request) {
//        JSONObject resultObj = new JSONObject();
//        try {
//
//            resultObj = v10GenericDataMigrationService.getCurrentDBTables(request);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return resultObj;
//    }
    
        @RequestMapping(value = {"/getTableLabels"})
    public @ResponseBody
    JSONObject getTableLabels(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

            resultObj = v10GenericDataMigrationService.getTableLabels(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
    
      @RequestMapping(value = {"/fetchYoutubeApiData"})
        public @ResponseBody
        JSONObject fetchYoutubeApiData(HttpServletRequest request) {
            JSONObject resultObj = new JSONObject();
            try {

                resultObj = v10GenericDataMigrationService.fetchYoutubeApiData(request);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return resultObj;
        }
    @RequestMapping(value = {"/fetchApiDataFromETLOS"})
    public @ResponseBody
    JSONObject fetchLinkedinApiData(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

            resultObj = v10GenericDataMigrationService.fetchApiDataFromETLOS(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = {"/getOnlineServicesHtml"})
    public @ResponseBody
    JSONObject getOnlineServicesHtml(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

            resultObj = v10GenericDataMigrationService.getOnlineServicesHtml(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

}
