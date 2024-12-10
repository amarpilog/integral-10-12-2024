/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;


import com.pilog.mdm.DAO.iVisionTransformConnectionTabsDAO;
import com.pilog.mdm.DAO.V10DataMigrationAccessDAO;
import com.pilog.mdm.utilities.PilogUtilities;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.sap.mw.jco.JCO;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
/**
 *
 * @author Ravindar.P
 */
@Service
public class iVisionTransformConnectionTabsService {

	@Value("${jdbc.driver}")
	private String dataBaseDriver;
	@Value("${jdbc.username}")
	private String userName;
	@Value("${jdbc.password}")
	private String password;
	@Value("${jdbc.url}")
	private String dbURL;

	@Autowired
	private iVisionTransformConnectionTabsDAO connectionTabsDAO;

	@Autowired
	private iVisionTransformConnectionTabsDAO visionTransformConnectionTabsDAO;

	private V10DataMigrationAccessDAO dataMigrationAccessDAO = new V10DataMigrationAccessDAO();

	
	
	public JSONObject getAvaliableConnections(HttpServletRequest request, String divId, String level, String fileType,
			String connectionType, String schemaObjectType, String connectionName, String connectionObj,
			String filterValue, String startIndex, String limit, String instance) {
		String htmlDiv = "";
		JSONObject resultObj = new JSONObject();
		try {
			
			if (divId != null && "files".equalsIgnoreCase(divId)) {
				resultObj = getAvailableFiles(request, level, fileType, filterValue, instance);
			} else if (divId != null && "Database".equalsIgnoreCase(divId)) {
				resultObj = getAvailableDataBaseConnections(request, level, connectionType, schemaObjectType,
						connectionName, connectionObj, filterValue, startIndex, limit, instance);
			} else if (divId != null && "ERP".equalsIgnoreCase(divId)) {
				// getAvailableERPConnections(level);
				resultObj = getAvailableERPConnections(request, level, connectionType, schemaObjectType, connectionName,
						connectionObj, filterValue, startIndex, limit, instance);
			} else if (divId != null && "OnlineServices".equalsIgnoreCase(divId)) {
				resultObj = getAvailableDataBaseConnections(request, level, connectionType, schemaObjectType,
						connectionName, connectionObj, filterValue, startIndex, limit, instance);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultObj;
	}

	public JSONObject getAvailableFiles(HttpServletRequest request, String level, String fileType, String filterValue, String instance) {
		String htmlDiv = "";
		String breadCrumb = "";
		JSONObject resultObj = new JSONObject();
		try {
		
			String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
			if (level != null && "0".equalsIgnoreCase(level)) {
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> <a href='#' class='connectionExplorerBreadCrumb'   > Files</a>";
				JSONObject infoObject = new JSONObject();
				String query = "SELECT DISTINCT FILE_TYPE FROM DAL_DM_SAVED_FILES WHERE USER_NAME='" + ssUsername + "'";
				
				infoObject.put("query", query);
				List dataList = connectionTabsDAO.getDataList(request, infoObject);
				if (dataList != null && !dataList.isEmpty()) {
					resultObj.put("resultList", dataList);
					resultObj.put("data-level", 1);
					resultObj.put("data-divId", "Files");

					resultObj.put("data-fileType", fileType);

					for (int i = 0; i < dataList.size(); i++) {
						String type = (String) dataList.get(i);
						htmlDiv += "<div class=\"etlConnectionItem etlFolder\" id=\"files\" data-level=\"1\" data-fileType=\"" + type
								+ "\" >" 
								+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "Files", type)+"'></sub></div>"
								+ "<div class='etlconnectionText'>" + type
								+ "</div>" 
								
								+ "</div>";
					}

				}
			} else if (level != null && ("1".equalsIgnoreCase(level) || "2".equalsIgnoreCase(level)) ) {
                               String processFlag =  request.getParameter("processFlag");
				//breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> Files <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+fileType+" files";
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"+"<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('Files','0','"+instance+"')\"> Files </a> <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> " + "<a href='#' class='connectionExplorerBreadCrumb'> "+ fileType + " files</a>";
				JSONObject infoObject = new JSONObject();
				String query = "SELECT FILE_ORG_NAME, FILE_NAME, FILE_PATH, FILE_TYPE, CREATE_DATE FROM DAL_DM_SAVED_FILES WHERE USER_NAME='"
						+ ssUsername + "' AND FILE_TYPE='" + fileType + "'";
				if (filterValue!=null && !"".equalsIgnoreCase(filterValue)) {
					if (filterValue.contains("%")) {
						query += " AND UPPER(FILE_ORG_NAME) LIKE '"+filterValue.toUpperCase()+"' ORDER BY FILE_ORG_NAME ";
					} else {
						query += " AND UPPER(FILE_ORG_NAME) = '"+filterValue.toUpperCase()+"' ORDER BY FILE_ORG_NAME ";
					}
					
				}else if(processFlag.equals("date")) {
                                    query += "ORDER BY CREATE_DATE DESC ";
                                }else if(processFlag.equals("name")){
                                    query += "ORDER BY FILE_ORG_NAME ";
                                }
				infoObject.put("query", query);
				List dataList = connectionTabsDAO.getDataList(request, infoObject);
				resultObj.put("resultList", dataList);
				resultObj.put("data-level", 2);
				resultObj.put("data-divId", "Files");
				resultObj.put("data-fileType", fileType);

				if (dataList != null && !dataList.isEmpty()) {

					String fileIcon = "";
					if (fileType != null && "xlsx".equalsIgnoreCase(fileType)) {
						fileIcon = "images/etl/xlsx-file.png";
					} else if (fileType != null && "xls".equalsIgnoreCase(fileType)) {
						fileIcon = "images/etl/xls-file.png";
					} else if (fileType != null && "pdf".equalsIgnoreCase(fileType)) {
						fileIcon = "images/pdficon.png";
					} else if (fileType != null && "csv".equalsIgnoreCase(fileType)) {
						fileIcon = "images/etl/csv-file.png";
					} else if (fileType != null && "xml".equalsIgnoreCase(fileType)) {
						fileIcon = "images/etl/xml-file.png";
					} else if (fileType != null && "jpg".equalsIgnoreCase(fileType)) {
						fileIcon = "images/doc2.png";
					} else {
						fileIcon = "images/doc2.png";
					}
					for (int i = 0; i < dataList.size(); i++) {
						Object[] rowData = (Object[]) dataList.get(i);

						String fileOrglName = (String) rowData[0];
						String fileName = (String) rowData[1];
						String filePath = (String) rowData[2];
                                                Timestamp timestamp = (Timestamp) rowData[4];
                                                Date date = new Date(timestamp.getTime());// Create a SimpleDateFormat object to specify the desired date format
                                                SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy hh:mm:ss a");// Format the Date as a String
                                                String formattedDate = dateFormat.format(date);
						JSONObject connObj = new JSONObject();
						connObj.put("fileName", fileOrglName);
						connObj.put("filePath", filePath + "/" + fileName);
						connObj.put("fileType", "." + fileType);
						connObj.put("imageIcon", fileIcon);
						
						htmlDiv += "<div class=\"etlConnectionItem etlFile\" id=\"Files\" data-connobj='" + connObj.toString()
								+ "' data-level=\"2\" data-fileType=\"" + fileType + "\" >"
								+ "<img class=\"etlexplorerImg\" src=\"" + fileIcon + "\"><span>" + fileOrglName + "<br><small>"+formattedDate+"</small></span>" 
								+ "<img class=\"etlexplorerRightClickOptions\" onclick=openConnectionItemRightClickConextMenu(event) onclick=openConnectionItemRightClickConextMenu(event) src='images/etl/threedots-ver.png'>"
								+ "</div>";
					}
				}

			}
			resultObj.put("breadCrumb", breadCrumb);
			resultObj.put("htmlDiv", htmlDiv);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultObj;
	}

	public JSONObject getAvailableERPConnections(HttpServletRequest request, String level, String connectionType,
			String schemaObjectType, String connectionName, String connectionObjStr, String filterValue,
			String startIndex, String limit, String instance) {
		String htmlDiv = "";
		String breadCrumb = "";
		JSONObject resultObj = new JSONObject();

		try {
			String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
			if (level != null && "0".equalsIgnoreCase(level)) {
				
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> <a href='#' class='connectionExplorerBreadCrumb'  > ERP</a>";
				JSONObject infoObject = new JSONObject();
				String query = "SELECT DISTINCT CONN_CUST_COL1 FROM DAL_DM_SAVED_CONNECTIONS WHERE USER_NAME='"
						+ ssUsername + "' AND CONN_CUST_COL2='ERP'";
				infoObject.put("query", query);
				List dataList = connectionTabsDAO.getDataList(request, infoObject);

				resultObj.put("resultList", dataList);
				resultObj.put("data-level", 1);
				resultObj.put("data-divId", "ERP");
				resultObj.put("data-conntype", connectionType);
				resultObj.put("data-connname", connectionName);
				resultObj.put("data-schemaobjecttype", schemaObjectType);
				resultObj.put("data-connobj", connectionObjStr);
				if (dataList != null && !dataList.isEmpty()) {

					for (int i = 0; i < dataList.size(); i++) {
						String connType = (String) dataList.get(i);
						//connType = connType.replaceAll("_", " ");
						htmlDiv += "<div class=\"etlConnectionItem etlFolder\" id=\"ERP\" data-level=\"1\" data-conntype=\"" + connType
								+ "\" >" 
								+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "ERP", connType)+"'></sub></div>"
								+ "<div class='etlconnectionText'>"
								+ connType.replaceAll("_", " ") + "</div>"   
								
								+ "</div>";
					}

				}
			} else if (level != null && "1".equalsIgnoreCase(level)) {
				//breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> ERP <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionType.replaceAll("_", " ");
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"+"<a href='#' class='connectionExplorerBreadCrumb' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('ERP','0','"+instance+"')\"> ERP </a> <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> " + "<a href='#' class='connectionExplorerBreadCrumb'> "+connectionType.replaceAll("_", " ")+"</a>";

				String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
				String query = "SELECT CONNECTION_NAME,"// 0
						+ " HOST_NAME,"// 1
						+ "CONN_PORT, "// 2
						+ "CONN_USER_NAME, "// 3
						+ "CONN_PASSWORD,"// 4
						+ "CONN_DB_NAME,"// 5
						+ "CONN_CUST_COL1,"// 6
						+ "CONN_CUST_COL4,"// 7
						+ "AUDIT_ID "// 8
						+ " FROM DAL_DM_SAVED_CONNECTIONS WHERE ORGN_ID ='" + orgnId + "' AND USER_NAME ='" + ssUsername
						+ "' AND CONN_CUST_COL1='" + connectionType + "' AND CONN_CUST_COL2='ERP'";
				JSONObject infoObject = new JSONObject();
				infoObject.put("query", query);
				List dataList = connectionTabsDAO.getDataList(request, infoObject);
				System.out.println("connectionsList::" + dataList);

				resultObj.put("resultList", dataList);
				resultObj.put("data-level", 2);
				resultObj.put("data-divId", "ERP");
				resultObj.put("data-conntype", connectionType);
				resultObj.put("data-connname", connectionName);
				resultObj.put("data-schemaobjecttype", schemaObjectType);
				resultObj.put("data-connobj", connectionObjStr);

				if (dataList != null && !dataList.isEmpty()) {

					for (int i = 0; i < dataList.size(); i++) {
						Object connObj[] = (Object[]) dataList.get(i);
						JSONObject connectionObj = new JSONObject();
						connectionObj.put("CONNECTION_NAME", connObj[0]);
						connectionObj.put("HOST_NAME", connObj[1]);
						connectionObj.put("CONN_PORT", connObj[2]);
						connectionObj.put("CONN_USER_NAME", connObj[3]);
						connectionObj.put("CONN_PASSWORD", connObj[4]);
						connectionObj.put("CONN_DB_NAME", connObj[5]);
						connectionObj.put("CONN_CUST_COL1", connObj[6]);

						String connName = String.valueOf(connObj[0]);
						htmlDiv += "<div class=\"etlConnectionItem etlFolder etlConnection\" id=\"ERP\" data-level=\"2\" data-connname=\"" + connName
								+ "\" data-conntype=\"" + connectionType + "\" data-connobj='"
								+ connectionObj.toString() + "'>"
								+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "ERP", connectionType)+"'></sub></div>"
								+ "<div class='etlconnectionText'>" + connName
								+ "</div>" 
								+ "<div class='etlconnectionThreedotDiv'><img class=\"etlexplorerRightClickOptions\" onclick=openConnectionItemRightClickConextMenu(event) src='images/etl/threedots-ver.png'></div>"
								+ "</div>";

					}
				}

			} else if (level != null && "2".equalsIgnoreCase(level)) {
				//breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> ERP <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionType.replaceAll("_", " ")+" <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionName;
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"+"<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('ERP','0','"+instance+"')\"> ERP </a>"+"<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"+"<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('ERP','1','"+instance+"')\"> " + connectionType.replaceAll("_", " ")+"</a>" + " <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> " + "<a href='#' class='connectionExplorerBreadCrumb'> "+connectionName+"</a>";

				List dataList = new ArrayList();
				dataList.add("TABLES");

				resultObj.put("resultList", dataList);
				resultObj.put("data-level", 3);

				resultObj.put("data-divId", "ERP");
				resultObj.put("data-conntype", connectionType);
				resultObj.put("data-connname", connectionName);
				resultObj.put("data-schemaobjecttype", schemaObjectType);
				resultObj.put("data-connobj", connectionObjStr);

				if (dataList != null && !dataList.isEmpty()) {

					String fileIcon = "";
					for (int i = 0; i < dataList.size(); i++) {
						String objectType = (String) dataList.get(i);
						htmlDiv += "<div class=\"etlConnectionItem etlFolder etlFolderSchemaObject\" id=\"ERP\" data-level=\"3\" data-connname=\""
								+ connectionName + "\"  data-conntype=\"" + connectionType
								+ "\" data-schemaobjecttype=\"" + objectType + "\" data-connobj='" + connectionObjStr
								+ "'>" 
								+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "ERP", connectionType)+"'></sub></div>"
								+ "<div class='etlconnectionText'>"
								+ objectType + "</div>" 
								+ "<div class='etlconnectionThreedotDiv'><img class=\"etlexplorerRightClickOptions\" onclick=openConnectionItemRightClickConextMenu(event) src='images/etl/threedots-ver.png'></div>"
								+ "</div>";
					}

				}

			} else if (level != null && ("3".equalsIgnoreCase(level) || "4".equalsIgnoreCase(level))) {
				//breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> ERP <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionType.replaceAll("_", " ")+" <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionName+" <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+schemaObjectType;
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"+"<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('ERP','0','"+instance+"')\"> ERP</a>"+"<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"+"<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('ERP','1','"+instance+"')\"> " + connectionType.replaceAll("_", " ")+"</a>" +"<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"+ "<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('ERP','2','"+instance+"')\"> " + connectionName +"</a>"+ " <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "
								+ "<a href='#' class='connectionExplorerBreadCrumb'> "+schemaObjectType+"</a>";
				JSONObject dbConnObj = (JSONObject) JSONValue.parse(connectionObjStr);
				JCO.Client connection = (JCO.Client) getConnection(dbConnObj);
				List dataList = getSapTablesList(request, connection, filterValue, startIndex, limit);

				resultObj.put("resultList", dataList);
				resultObj.put("data-level", 4);
				resultObj.put("data-divId", "ERP");
				resultObj.put("data-conntype", connectionType);
				resultObj.put("data-connname", connectionName);
				resultObj.put("data-schemaobjecttype", schemaObjectType);
				resultObj.put("data-connobj", connectionObjStr);

				if (dataList != null && !dataList.isEmpty()) {

					for (int i = 0; i < dataList.size(); i++) {
						String tableName = (String) dataList.get(i);
						htmlDiv += "<div class=\"etlConnectionItem etlSchemaObject etlERPSchemaObject\" id=\"ERP\" data-level=\"4\" data-schemaobjectname=\""
								+ tableName + "\" data-conntype=\"" + connectionType + "\" data-schemaobjecttype=\""
								+ schemaObjectType + "\" data-connobj='" + connectionObjStr + "' >"
								+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/GridDB.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "ERP", connectionType)+"'></sub></div>"
								+ "<div class='etlconnectionText'>" + tableName
								+ "</div>" 
								+ "<div class='etlconnectionThreedotDiv'><img class=\"etlexplorerRightClickOptions\" onclick=openConnectionItemRightClickConextMenu(event) src='images/etl/threedots-ver.png'></div>"
								+ "</div>";
					}

				}
			}
			resultObj.put("breadCrumb", breadCrumb);
			resultObj.put("htmlDiv", htmlDiv);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultObj;
	}

	public JSONObject getAvailableDataBaseConnections(HttpServletRequest request, String level, String connectionType,
			String schemaObjectType, String connectionName, String connectionObjStr, String filterValue,
			String startIndex, String limit, String instance) {
		String htmlDiv = "";
		String breadCrumb = "";
		String funcProcFlag = "";
                
		JSONObject resultObj = new JSONObject();
		try {
			String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
			if (level != null && "0".equalsIgnoreCase(level)) {
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> <a href='#' class='connectionExplorerBreadCrumb'  > Database</a>";
				JSONObject infoObject = new JSONObject();
				String query = "SELECT DISTINCT CONN_CUST_COL1 FROM DAL_DM_SAVED_CONNECTIONS WHERE USER_NAME='"
						+ ssUsername + "' AND CONN_CUST_COL2='DB'";
				infoObject.put("query", query);
				List dataList = connectionTabsDAO.getDataList(request, infoObject); 
			
				if (dataList == null || dataList.isEmpty()) {
					String connType = "Oracle";
					htmlDiv += "<div class=\"etlConnectionItem etlFolder\" id=\"Database\" data-level=\"1\" data-conntype=\""
							+ connType + "\" >"
							+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
							+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "Database", connType)+"'></sub></div>"
							+ "<div class='etlconnectionText'>" + connType
							+ "</div>" + "</div>";  
				}
	
				if (dataList != null && !dataList.isEmpty()) {
					resultObj.put("resultList", dataList);
					resultObj.put("data-level", 1);
					resultObj.put("data-divId", "Database");
					resultObj.put("data-conntype", connectionType);
					resultObj.put("data-connname", connectionName);
					resultObj.put("data-schemaobjecttype", schemaObjectType);
					resultObj.put("data-connobj", connectionObjStr);

					for (int i = 0; i < dataList.size(); i++) {
						String connType = (String) dataList.get(i);
						htmlDiv += "<div class=\"etlConnectionItem etlFolder\" id=\"Database\" data-level=\"1\" data-conntype=\""
								+ connType + "\" >"
								+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "Database", connType)+"'></sub></div>"
								+ "<div class='etlconnectionText'>" + connType
								+ "</div>" + "</div>";
					}

				}
			} else if (level != null && "1".equalsIgnoreCase(level)) {
				//breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> Database <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionType;
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"+"<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('Database','0','"+instance+"')\"> Database</a>"+" <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> " + "<a href='#' class='connectionExplorerBreadCrumb'> "+connectionType+"</a>";

				String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
				String query = "SELECT CONNECTION_NAME,"// 0
						+ " HOST_NAME,"// 1
						+ "CONN_PORT, "// 2
						+ "CONN_USER_NAME, "// 3
						+ "CONN_PASSWORD,"// 4
						+ "CONN_DB_NAME,"// 5
						+ "CONN_CUST_COL1,"// 6
						+ "CONN_CUST_COL4,"// 7
						+ "AUDIT_ID "// 8
						+ " FROM DAL_DM_SAVED_CONNECTIONS WHERE ORGN_ID ='" + orgnId + "' AND USER_NAME ='" + ssUsername
						+ "' AND CONN_CUST_COL1='" + connectionType + "' AND CONN_CUST_COL2='DB'";
				JSONObject infoObject = new JSONObject();
				infoObject.put("query", query);
				List dataList = connectionTabsDAO.getDataList(request, infoObject);
				System.out.println("connectionsList::" + dataList);
			
				if ( dataBaseDriver.toUpperCase().contains(connectionType.toUpperCase()) ) {
					JSONObject currentConnObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName,
							password, "Current_V10");
					if (currentConnObj != null) {
						htmlDiv += "<div class=\"etlConnectionItem etlFolder etlCurrentConnection\" id=\"Database\" data-level=\"2\" data-connname=\""
								+ currentConnObj.get("CONNECTION_NAME") + "\" data-conntype=\""
								+ currentConnObj.get("CONN_CUST_COL1") + "\" data-connobj='" + currentConnObj.toString()
								+ "'>" + "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "Database", connectionType)+"'></sub></div>"
								+ "<div class='etlconnectionThreedotDiv'>"
								+ "Current Connection" + "</div>" + "</div>";
					}
				}
				
				if (dataList != null && !dataList.isEmpty()) {
					resultObj.put("resultList", dataList);
					resultObj.put("data-level", 2);
					resultObj.put("data-divId", "Database");
					resultObj.put("data-conntype", connectionType);
					resultObj.put("data-connname", connectionName);
					resultObj.put("data-schemaobjecttype", schemaObjectType);
					resultObj.put("data-connobj", connectionObjStr);

					for (int i = 0; i < dataList.size(); i++) {
						Object connObj[] = (Object[]) dataList.get(i);
						JSONObject connectionObj = new JSONObject();
						connectionObj.put("CONNECTION_NAME", connObj[0]);
						connectionObj.put("HOST_NAME", connObj[1]);
						connectionObj.put("CONN_PORT", connObj[2]);
						connectionObj.put("CONN_USER_NAME", connObj[3]);
						connectionObj.put("CONN_PASSWORD", connObj[4]);
						connectionObj.put("CONN_DB_NAME", connObj[5]);
						connectionObj.put("CONN_CUST_COL1", connObj[6]);

						String connName = String.valueOf(connObj[0]);
						htmlDiv += "<div class=\"etlConnectionItem etlFolder etlConnection\" id=\"Database\" data-level=\"2\" data-connname=\""
								+ connName + "\" data-conntype=\"" + connectionType + "\" data-connobj='"
								+ connectionObj.toString() + "'>"
								+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"+getExlporerSubImage(request, "Database", connectionType)+"'></sub></div>"
								+ "<div class='etlconnectionText'>" + connName
								+ "</div>" 
								+ "<div class='etlconnectionThreedotDiv'><img class=\"etlexplorerRightClickOptions\" onclick=openConnectionItemRightClickConextMenu(event) src='images/etl/threedots-ver.png'></div>"
								+ "</div>";

					}
				}

			} else if (level != null && "2".equalsIgnoreCase(level)) {
				// breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> Database <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionType+" <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionName;
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"
						+ "<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('Database','0','"
						+ instance + "')\"> Database</a>" + "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"
						+ "<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('Database','1','"
						+ instance + "')\"> " + connectionType + "</a>" + " <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "
						+ "<a href='#' class='connectionExplorerBreadCrumb'> " + connectionName + "</a>";

				List dataList = new ArrayList();
				if (connectionName != null && connectionName.equalsIgnoreCase("Current_V10")) {
					dataList.add("TABLES");
				} else if(connectionType != null && connectionType.equalsIgnoreCase("MongoDb")) {
					dataList.add("COLLECTIONS");
				} else {
					dataList.add("TABLES");
					dataList.add("VIEWS");
					dataList.add("SYSNONYMS");
					dataList.add("FUNCTIONS");
					dataList.add("PROCEDURES");
				}

				if (dataList != null && !dataList.isEmpty()) {
					resultObj.put("resultList", dataList);
					resultObj.put("data-level", 3);
					resultObj.put("data-divId", "Database");
					resultObj.put("data-conntype", connectionType);
					resultObj.put("data-connname", connectionName);
					resultObj.put("data-schemaobjecttype", schemaObjectType);
					resultObj.put("data-connobj", connectionObjStr);

					String fileIcon = "";
					for (int i = 0; i < dataList.size(); i++) {
						String objectType = (String) dataList.get(i);
						htmlDiv += "<div class=\"etlConnectionItem etlFolder etlFolderSchemaObject\" id=\"Database\" data-level=\"3\" data-connname=\""
								+ connectionName + "\"  data-conntype=\"" + connectionType
								+ "\" data-schemaobjecttype=\"" + objectType + "\" data-connobj='" + connectionObjStr
								+ "'>" + "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/File-Icon.svg\">"
								+ "<sub><img class=\"etlexplorerSubImg\" src='"
								+ getExlporerSubImage(request, "Database", connectionType) + "'></sub></div>" + "<div class='etlconnectionText'>"
								+ objectType + "</div>"
								+ "<div class='etlconnectionThreedotDiv'><img class=\"etlexplorerRightClickOptions\" onclick=openConnectionItemRightClickConextMenu(event) src='images/etl/threedots-ver.png'></div>"
								+ "</div>";
					}

				}

			} else if (level != null && ("3".equalsIgnoreCase(level) || "4".equalsIgnoreCase(level))) {
				
				Boolean NoSQL = false;

				// breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> Database <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionType+" <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "+connectionName+" <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>
				// "+schemaObjectType;
				breadCrumb = "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"
						+ "<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('Database','0','"
						+ instance + "')\"> Database</a>" + "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"
						+ "<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('Database','1','"
						+ instance + "')\"> " + connectionType + "</a>" + "<i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i>"
						+ "<a href='#' class='connectionExplorerBreadCrumb'  onclick =\"fetchFolderData('Database','2','"
						+ instance + "')\"> " + connectionName + "</a>" + " <i class=\"fa fa-angle-right\" aria-hidden=\"true\"></i> "
						+ "<a href='#' class='connectionExplorerBreadCrumb'> " + schemaObjectType + "</a>";

				JSONObject dbConnObj = (JSONObject) JSONValue.parse(connectionObjStr);
				
				JSONObject infoObject = new JSONObject();
				String query = "";
				if ("ORACLE".equalsIgnoreCase(String.valueOf(dbConnObj.get("CONN_CUST_COL1")))) {  

					String fieldName = "";
					String table = "";
					String whereCond = " WHERE ";
					if (schemaObjectType != null && "TABLES".equalsIgnoreCase(schemaObjectType)) {
						fieldName = "TABLE_NAME";
						table = "USER_TABLES";
					} else if (schemaObjectType != null && "VIEWS".equalsIgnoreCase(schemaObjectType)) {
						fieldName = "VIEW_NAME";
						table = "USER_VIEWS";
					} else if (schemaObjectType != null && "SYSNONYMS".equalsIgnoreCase(schemaObjectType)) {
						fieldName = "SYNONYM_NAME";
						table = "USER_SYNONYMS";
					} else if (schemaObjectType != null && ("FUNCTIONS".equalsIgnoreCase(schemaObjectType))
							|| ("PROCEDURES".equalsIgnoreCase(schemaObjectType))) {
						fieldName = "OBJECT_NAME";
						table = "USER_OBJECTS";
						whereCond = " AND ";
						// isFuncOrProc = "Y";
						funcProcFlag = "FUNCTIONS".equalsIgnoreCase(schemaObjectType) ? "F" : "P";

					}

					if (connectionName != null && connectionName.equalsIgnoreCase("Current_V10")) {
						query = "SELECT TABLE_NAME FROM  C_ETL_DAL_AUTHORIZATION";
					} else {
						if (schemaObjectType != null && ("FUNCTIONS".equalsIgnoreCase(schemaObjectType)
								|| "PROCEDURES".equalsIgnoreCase(schemaObjectType))) {
							query = "SELECT DISTINCT " + fieldName + " FROM  " + table + " WHERE OBJECT_TYPE = '"
									+ schemaObjectType.substring(0, schemaObjectType.length() - 1) + "'";
						} else {
							query = "SELECT DISTINCT " + fieldName + " FROM  " + table;
						}

					}

					if (filterValue != null && !"".equalsIgnoreCase(filterValue)) {
						if (filterValue.contains("%")) {
							query += whereCond + fieldName + " LIKE '" + filterValue.toUpperCase() + "' ";
						} else {
							query += whereCond + fieldName + " = '" + filterValue.toUpperCase() + "' ";
						}

					}
					query += " ORDER BY " + fieldName + "";

					if (startIndex != null && !"".equalsIgnoreCase(startIndex)) {
						query += " OFFSET " + startIndex + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
					}

				} else if ("MYSQL".equalsIgnoreCase(String.valueOf(dbConnObj.get("CONN_CUST_COL1")))) {
					String fieldName = "";

					if (schemaObjectType != null && ("FUNCTIONS".equalsIgnoreCase(schemaObjectType)
							|| "PROCEDURES".equalsIgnoreCase(schemaObjectType))) {
						String routineType = "FUNCTIONS".equalsIgnoreCase(schemaObjectType) ? "FUNCTION" : "PROCEDURE";
						query = "SELECT ROUTINE_NAME " + "FROM INFORMATION_SCHEMA.ROUTINES " + "WHERE ROUTINE_TYPE = '"
								+ routineType + "' " + "AND ROUTINE_SCHEMA = '" + dbConnObj.get("CONN_DB_NAME") + "'"; // 'MDRMV10'";
																														// //your_database_name
						fieldName = "ROUTINE_NAME";
						funcProcFlag = "FUNCTIONS".equalsIgnoreCase(schemaObjectType) ? "F" : "P";
					} else {
						if (schemaObjectType != null && "TABLES".equalsIgnoreCase(schemaObjectType)) {
							query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS "
									+ " WHERE TABLE_SCHEMA = '" + dbConnObj.get("CONN_DB_NAME") + "'";
						} else {
							query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS "
									+ " WHERE TABLE_SCHEMA = '" + dbConnObj.get("CONN_DB_NAME") + "'";
						}
						// + " AND COLUMN_NAME='TABLE_NAME' ";
						fieldName = "TABLE_NAME";
					}

					if (filterValue != null && !"".equalsIgnoreCase(filterValue)) {
						if (filterValue.contains("%")) {
							query += " AND " + fieldName + " LIKE '" + filterValue.toUpperCase() + "' ";
						} else {
							query += " AND " + fieldName + " = '" + filterValue.toUpperCase() + "' ";
						}
					}
					query += " ORDER BY " + fieldName;

					if (startIndex != null && !"".equalsIgnoreCase(startIndex)) {
						query += " LIMIT " + startIndex + "," + limit;
					}

				} else if ("SQLSERVER".equalsIgnoreCase(String.valueOf(dbConnObj.get("CONN_CUST_COL1")))) {
					String fieldName = "";
					if (schemaObjectType != null && "FUNCTIONS".equalsIgnoreCase(schemaObjectType)) {
						fieldName = "FUNCTION_NAME";
						query = "SELECT name AS FUNCTION_NAME " + "FROM sys.objects "
								+ "WHERE type_desc LIKE '%FUNCTION%'";
						funcProcFlag = "F";
					} else if (schemaObjectType != null && "PROCEDURES".equalsIgnoreCase(schemaObjectType)) {
						fieldName = "PROCEDURE_NAME";
						query = "SELECT name AS PROCEDURE_NAME " + "FROM sys.objects " + "WHERE type = 'P'";
						funcProcFlag = "P";
					} else {
						if (schemaObjectType != null && "TABLES".equalsIgnoreCase(schemaObjectType)) {
							query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS "
									+ " WHERE TABLE_SCHEMA = '" + dbConnObj.get("CONN_DB_NAME") + "'";
							fieldName = "TABLE_NAME";
						} else if (schemaObjectType != null && "VIEWS".equalsIgnoreCase(schemaObjectType)) {
							query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE "
									+ " TABLE_SCHEMA = '" + dbConnObj.get("CONN_DB_NAME") + "'";
							fieldName = "TABLE_NAME";
							// + " AND COLUMN_NAME='TABLE_NAME' ";
						} else {
							query = "SELECT name, base_object_name FROM sys.synonyms";
							fieldName = "name";
						}

					}

					if (filterValue != null && !"".equalsIgnoreCase(filterValue)) {
						if (filterValue.contains("%")) {
							query += " AND " + fieldName + " LIKE '" + filterValue.toUpperCase() + "' ";
						} else {
							query += " AND " + fieldName + " = '" + filterValue.toUpperCase() + "' ";
						}
					}
					query += " ORDER BY " + fieldName;

					if (startIndex != null && !"".equalsIgnoreCase(startIndex)) {
						query += " OFFSET " + startIndex + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
					}
				} else if ("MongoDb".equalsIgnoreCase(String.valueOf(dbConnObj.get("CONN_CUST_COL1")))) {
					NoSQL = true;
					String connectionString = "mongodb://" + dbConnObj.get("HOST_NAME") + ":" + dbConnObj.get("CONN_PORT") + "/";
			        String dbName = (String) dbConnObj.get("CONN_DB_NAME");
			        MongoClient mongoClient = null;
			        try  {
			        	mongoClient = MongoClients.create(new ConnectionString(connectionString));
			            MongoDatabase database = mongoClient.getDatabase(dbName);
			            List<String> collectionList = new ArrayList();
			            
			            if (filterValue != null && !"".equalsIgnoreCase(filterValue)) {
			            	boolean collectionExists = false;
			                MongoIterable<String> collectionNames = database.listCollectionNames();
			                for (String collectionName : collectionNames) {
			                    if (collectionName.equals(filterValue)) {
			                        collectionExists = true;
			                        collectionList.add(collectionName);
			                        break;
			                    }
			                }

			                if (!collectionExists) {
			                	System.out.println("Collection not found: " + filterValue);
			                }
						} else {

							// Fetch the collection names
							MongoIterable<String> collectionNames = database.listCollectionNames();

							for (String collectionName : collectionNames) {
								collectionList.add(collectionName);
							}
						}
			            
			            resultObj.put("resultList", collectionList);
						resultObj.put("data-level", 4);
						resultObj.put("data-divId", "Database");
						resultObj.put("data-conntype", connectionType);
						resultObj.put("data-connname", connectionName);
						resultObj.put("data-schemaobjecttype", schemaObjectType);
						resultObj.put("data-connobj", connectionObjStr);

			            System.out.println("Collections in " + dbName + ":");
			            for (String collectionName : collectionList) {
			                System.out.println(collectionName);
							
							htmlDiv += "<div class=\"etlConnectionItem etlSchemaObject\" id=\"Database\" data-level=\"4\" data-schemaobjectname=\""
									+ collectionName + "\" data-conntype=\""
									+ connectionType + "\" data-schemaobjecttype=\"" + schemaObjectType
									+ "\" data-connobj='" + connectionObjStr + "'>"
									// + "' ondblclick='viewScemaObjectData(event)' ' >"
									+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/GridDB.svg\">"
									+ "<sub><img class=\"etlexplorerSubImg\" src='" + getExlporerSubImage(request, "Database", connectionType) + "'></sub></div>"
									+ "<div class='etlconnectionText'>"
									+ collectionName + "</div>"
									+ "<div class='etlconnectionThreedotDiv'><img class=\"etlexplorerRightClickOptions\" onclick=openConnectionItemRightClickConextMenu(event) src='images/etl/threedots-ver.png'></div>"
									+ "</div>";
			            }
			        } catch (Exception e) {
			            e.printStackTrace();
			        } finally {
			            // Close the MongoClient in a finally block to ensure it gets closed even if an exception occurs
			            if (mongoClient != null) {
			                mongoClient.close();
			            }
			        }
				}
				

				if (!NoSQL) {
					System.out.println("query :: " + query);
					Connection connection = (Connection) getConnection(dbConnObj);
					PreparedStatement preparedStatement = connection.prepareStatement(query);
					ResultSet resultSet = preparedStatement.executeQuery();
					List dataList = new ArrayList();
					while (resultSet.next()) {

						String tableName = (String) resultSet.getObject(1);

						dataList.add(tableName);
					}

					resultObj.put("resultList", dataList);
					resultObj.put("data-level", 4);
					resultObj.put("data-divId", "Database");
					resultObj.put("data-conntype", connectionType);
					resultObj.put("data-connname", connectionName);
					resultObj.put("data-schemaobjecttype", schemaObjectType);
					resultObj.put("data-connobj", connectionObjStr);

					if (dataList != null && !dataList.isEmpty()) {

						for (int i = 0; i < dataList.size(); i++) {
							String tableName = (String) dataList.get(i);
							String functionName = ("FUNCTIONS".equalsIgnoreCase(schemaObjectType)
									|| "PROCEDURES".equalsIgnoreCase(schemaObjectType))
											? "'  ondblclick=viewTableInfo('" + connectionName + "','" + tableName
													+ "','" + dbConnObj + "','" + funcProcFlag + "')  >"
											: "'  ondblclick='viewScemaObjectData(event)' ' >";
							htmlDiv += "<div class=\"etlConnectionItem etlSchemaObject\" id=\"Database\" data-level=\"4\" data-schemaobjectname=\""
									+ tableName + "\" func-proc-flag=\"" + funcProcFlag + "\" data-conntype=\""
									+ connectionType + "\" data-schemaobjecttype=\"" + schemaObjectType
									+ "\" data-connobj='" + connectionObjStr + functionName
									// + "' ondblclick='viewScemaObjectData(event)' ' >"
									+ "<div class='etlConnnectionlistIcon'><img class=\"etlexplorerImg\" src=\"images/GridDB.svg\">"
									+ "<sub><img class=\"etlexplorerSubImg\" src='"
									+ getExlporerSubImage(request, "Database", connectionType) + "'></sub></div>" + "<div class='etlconnectionText'>"
									+ tableName + "</div>"
									+ "<div class='etlconnectionThreedotDiv'><img class=\"etlexplorerRightClickOptions\" onclick=openConnectionItemRightClickConextMenu(event) src='images/etl/threedots-ver.png'></div>"
									+ "</div>";
						}

					}
				}
			}

			resultObj.put("htmlDiv", htmlDiv);
			resultObj.put("breadCrumb", breadCrumb);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultObj;
	}

	public List getSapTablesList(HttpServletRequest request, JCO.Client connection, String filterTableVal,
			String startIndex, String limit) {
		List dataList = new ArrayList();
		dataMigrationAccessDAO.retrieveRepository(connection);
		JCO.Table theConnection = null;
		String tableDataStr = "";
		JCO.Table countTableData = null;
		JCO.Table fetchTableData = null;
		JCO.Table countFields = null;
		JCO.Table dataFields = null;
		JCO.Function countFunction = null;
		JCO.Function dataFunction = null;
		JSONArray treeDataObjArray = new JSONArray();
		try {
			if (filterTableVal != null && !"".equalsIgnoreCase(filterTableVal)
					&& !"null".equalsIgnoreCase(filterTableVal)) {
				filterTableVal = filterTableVal.toUpperCase();
			}
			long count = 0;
			String tabClass = "";

			String filterTableCond = "";
			if (filterTableVal!=null && !"".equalsIgnoreCase(filterTableVal) &&  filterTableVal.contains("%")) {
				filterTableCond = "LIKE";
			} else {
				filterTableCond = "=";
			}
			
			String parentKeyData = "TABLES";
			String showMoreIcon = "";

			if (filterTableCond != null && !"".equalsIgnoreCase(filterTableCond)
					&& "!=".equalsIgnoreCase(filterTableCond)) {
				filterTableCond = "<>";
			}
			if (parentKeyData != null && !"".equalsIgnoreCase(parentKeyData) && !"".equalsIgnoreCase(parentKeyData)) {
				if ("TABLES".equalsIgnoreCase(parentKeyData)) {
					tabClass = " TABCLASS EQ 'TRANSP' ";
				} else if ("VIEWS".equalsIgnoreCase(parentKeyData)) {
					tabClass = " TABCLASS EQ 'VIEW' ";
				}
				countFunction = dataMigrationAccessDAO.getFunction("RFC_READ_TABLE");
				dataFunction = dataMigrationAccessDAO.getFunction("RFC_READ_TABLE");
				if (filterTableVal != null && !filterTableVal.isEmpty()) {
					JCO.ParameterList countListParams = countFunction.getImportParameterList();
					countListParams.setValue("DD02L", "QUERY_TABLE");// For MARA
					countListParams.setValue("|", "DELIMITER");
					countTableData = countFunction.getTableParameterList().getTable("DATA");
					countFields = countFunction.getTableParameterList().getTable("FIELDS");
					JCO.Table countOption = countFunction.getTableParameterList().getTable("OPTIONS");
					countFields.appendRow();
					countFields.setValue("TABNAME", "FIELDNAME");
					countOption.appendRow();
//	                    countOption.setValue("TABNAME " + filterTableCond + " '" + filterTableVal + "' AND (" + tabClass + ")", "TEXT");
					countOption.setValue("(" + tabClass + ")", "TEXT");
					countOption.setValue("TABNAME " + filterTableCond + " '" + filterTableVal + "'", "TEXT");
//	                  
					connection.execute(countFunction);
					count = countTableData.getNumRows();

					JCO.ParameterList dataListParams = dataFunction.getImportParameterList();
					dataListParams.setValue("DD02L", "QUERY_TABLE");// For MARA
					dataListParams.setValue("|", "DELIMITER");
					fetchTableData = dataFunction.getTableParameterList().getTable("DATA");
					dataFields = dataFunction.getTableParameterList().getTable("FIELDS");
					JCO.Table fetchOptions = dataFunction.getTableParameterList().getTable("OPTIONS");
					dataFields.appendRow();
					dataFields.setValue("TABNAME", "FIELDNAME");
					fetchOptions.appendRow();
					fetchOptions.setValue("(" + tabClass + ")", "TEXT");
					fetchOptions.setValue("TABNAME " + filterTableCond + " '" + filterTableVal + "'", "TEXT");
//	                    fetchOptions.setValue(" ", "TEXT");
					dataListParams.setValue(startIndex, "ROWSKIPS");
					dataListParams.setValue(limit, "ROWCOUNT");
					connection.execute(dataFunction);

				} else {
					JCO.ParameterList countListParams = countFunction.getImportParameterList();
					countListParams.setValue("DD02L", "QUERY_TABLE");// For MARA
					countListParams.setValue("|", "DELIMITER");
					countTableData = countFunction.getTableParameterList().getTable("DATA");
					countFields = countFunction.getTableParameterList().getTable("FIELDS");
					JCO.Table countOption = countFunction.getTableParameterList().getTable("OPTIONS");
					countFields.appendRow();
					countFields.setValue("TABNAME", "FIELDNAME");
					countOption.appendRow();
					countOption.setValue("(" + tabClass + ")", "TEXT");
//	                     countOption.setValue("TABNAME NOT LIKE '/%'", "TEXT");
					connection.execute(countFunction);
					count = countTableData.getNumRows();

					JCO.ParameterList dataListParams = dataFunction.getImportParameterList();
					dataListParams.setValue("DD02L", "QUERY_TABLE");// For MARA
					dataListParams.setValue("|", "DELIMITER");
					dataListParams.setValue(startIndex, "ROWSKIPS");
					dataListParams.setValue(limit, "ROWCOUNT");
					fetchTableData = dataFunction.getTableParameterList().getTable("DATA");
					dataFields = dataFunction.getTableParameterList().getTable("FIELDS");
					JCO.Table dataOption = dataFunction.getTableParameterList().getTable("OPTIONS");
					dataFields.appendRow();
					dataFields.setValue("TABNAME", "FIELDNAME");
					dataOption.appendRow();
					dataOption.setValue("(" + tabClass + ")", "TEXT");
//	                    dataOption.setValue("TABNAME NOT LIKE '/%'", "TEXT"); // RAVI SAP FILTER / TABLES
					connection.execute(dataFunction);
				}
				if (fetchTableData != null) {
					JSONObject treeObj = new JSONObject();
					String tablename = fetchTableData.getString("WA") != null
							? fetchTableData.getString("WA").toUpperCase()
							: "";
					dataList.add(tablename);
				}
				while (fetchTableData.nextRow()) {
					JSONObject treeObj = new JSONObject();
					String tablename = fetchTableData.getString("WA") != null
							? fetchTableData.getString("WA").toUpperCase()
							: "";
					dataList.add(tablename);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataList;
	}

	public Object getConnection(JSONObject dbObj) {

		Connection connection = null;
		Object returnedObj = null;
		try {
			if (dbObj != null && !dbObj.isEmpty()) {
				String initParamClassName = "com.pilog.mdm.transformaccess.V10MigrationDataAccess";
				String initParamMethodName = "get" + dbObj.get("CONN_CUST_COL1") + "Connection";
				System.out.println(
						" initParamClassName:" + initParamClassName + "initParamMethodName:" + initParamMethodName);
				Class clazz = Class.forName(initParamClassName);
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1"))) || "SAP_HANA".equalsIgnoreCase(String.valueOf(dbObj.get("CONN_CUST_COL1")))) {
					Class<?>[] paramTypes = { String.class, String.class, String.class, String.class, String.class,
							String.class, String.class };
					Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
					Object targetObj = new PilogUtilities().createObjectByClass(clazz);
					returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("CONN_PORT")),
							String.valueOf(dbObj.get("CONN_USER_NAME")), String.valueOf(dbObj.get("CONN_PASSWORD")),
							"EN", String.valueOf(dbObj.get("HOST_NAME")), String.valueOf(dbObj.get("CONN_DB_NAME")),
							String.valueOf(dbObj.get("GROUP")));
				} else {
					Class<?>[] paramTypes = { String.class, String.class, String.class, String.class, String.class };
					Method method = clazz.getMethod(initParamMethodName.trim(), paramTypes);
					Object targetObj = new PilogUtilities().createObjectByClass(clazz);
					returnedObj = method.invoke(targetObj, String.valueOf(dbObj.get("HOST_NAME")),
							String.valueOf(dbObj.get("CONN_PORT")), String.valueOf(dbObj.get("CONN_USER_NAME")),
							String.valueOf(dbObj.get("CONN_PASSWORD")), String.valueOf(dbObj.get("CONN_DB_NAME")));

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnedObj;
	}
	
	public String getExlporerSubImage(HttpServletRequest request,  String divType, String connType) {
		String explorerSubImage = "";
		connType = connType.toUpperCase();
		divType = divType.toUpperCase();
		try {
			if (divType.equalsIgnoreCase("DATABASE")) {
				if (connType.equalsIgnoreCase("ORACLE")) {
					explorerSubImage = "images/DM_ORACLE-Icon.svg";
				} else if (connType.equalsIgnoreCase("MYSQL")) {
					explorerSubImage = "images/MYSQL_Icon.png";
				} else if (connType.equalsIgnoreCase("SQLSERVER")) {
					explorerSubImage = "images/MYSQL_Icon.png";
				} else if (connType.equalsIgnoreCase("MYSQL")) {
					explorerSubImage = "images/MYSQL_Icon.png";
				} else if (connType.equalsIgnoreCase("MongoDb")) {
					explorerSubImage = "images/etl/mongodb.png";
				}else {
					explorerSubImage = "images/ETL_sourceDB.png";
				}
			}  else if (divType.equalsIgnoreCase("FILES")) {
				if (connType.equalsIgnoreCase("XLSX")) {
					explorerSubImage = "images/xlsicon.png";
				} else if (connType.equalsIgnoreCase("XLS")) {
					explorerSubImage = "images/xlsicon.png";
				} else if (connType.equalsIgnoreCase("XML")) {
					explorerSubImage = "images/xml_icon.png";
				} else if (connType.equalsIgnoreCase("JSON")) {
					explorerSubImage = "images/JSON_Isons-02.svg";
				}  else if (connType.equalsIgnoreCase("CSV")) {
					explorerSubImage = "images/CSV%20ICON-01.svg";
				} else {
					explorerSubImage = "images/XLSX-Icon.svg";
				}
			} else if (divType.equalsIgnoreCase("ERP")) {
				if (connType.equalsIgnoreCase("SAP_ECC")) {
					explorerSubImage = "images/SAP_Ison-01.png";
				} else if (connType.equalsIgnoreCase("SAP_HANA")) {
					explorerSubImage = "images/etl/SAP_HANA.jpg";
				} else if (connType.equalsIgnoreCase("ORACLE ERP")) {
					explorerSubImage = "images/DM_ORA_ERP-Icon-01.png";
				} else {
					explorerSubImage = "images/SAP_Ison-01.png";
				}
			} else if (divType.equalsIgnoreCase("ONLINE SERVICES")) {
				if (connType.equalsIgnoreCase("LINKEDIN")) {
					explorerSubImage = "images/Linkedin-Icon-01.png";
				} else if (connType.equalsIgnoreCase("FACEBOOK")) {
					explorerSubImage = "images/FB-Icon-01.png";
				} else if (connType.equalsIgnoreCase("TWITTER")) {
					explorerSubImage = "images/TWITTER-Icon.svg";
				} else {
					explorerSubImage = "images/ONLINE_SERVICES_Icon.svg";
				}
			}
			
			
		} catch(Exception ex) {
			
		}
		return explorerSubImage;
	}

}
