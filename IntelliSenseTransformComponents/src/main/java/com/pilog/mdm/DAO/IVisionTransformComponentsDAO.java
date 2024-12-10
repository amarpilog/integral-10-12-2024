/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;

import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.utilities.PilogUtilities;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Blob;
import oracle.sql.BLOB;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.sql.rowset.serial.SerialBlob;

import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;


/**
 *
 * @author Ravindar.P
 */
@Repository
public class IVisionTransformComponentsDAO {

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
	    public List getComponentInfo(HttpServletRequest request, String compName) {

	        List dataList = new ArrayList();
	        Map<String, Object> map = new HashMap<>();
	        try {
	            String query = "SELECT DATA_OPTITLE, IMAGE, COMPONENT_DESCRIPTION, COMPONENT_VIDEO FROM C_ETL_COMPONENTS WHERE DATA_TYPE=:DATA_TYPE ";
	            map.put("DATA_TYPE", compName);
	            System.out.println(" query ::: " + query);

	            dataList = access.sqlqueryWithParams(query, map);
	            
	        } catch (Exception e) {
	            e.printStackTrace();
	            
	        }
	        return dataList;

	    }
	    
	    @Transactional
	    public List getDataListFromQuery(HttpServletRequest request, String query, Map map) {

	        List dataList = new ArrayList();
	        try {
	            
	            System.out.println(" query ::: " + query);

	            dataList = access.sqlqueryWithParams(query, map);
	            
	        } catch (Exception e) {
	            e.printStackTrace();
	            
	        }
	        return dataList;

	    }
	   
	    @Transactional
	    public List<String> fetchMaskedColumnsList(HttpServletRequest request, String tableName, String host, String ssUsername){
	    	 List<String> columnsList = new ArrayList<>();
	         try {
	        	 
	             String selectQuery = "SELECT "
	             		+ " COLUMN_NAME "
	             		+ " FROM C_ETL_DATA_MASKING"
	                     + " WHERE USER_NAME =:USER_NAME "
	                     + " AND USER_NAME =:USER_NAME "
	                     + " AND CONNECTION_HOST =:CONNECTION_HOST "
	                     + " AND TABLE_NAME =:TABLE_NAME ";
	                   
	             Map selectMap = new HashMap();
	             selectMap.put("USER_NAME", ssUsername);
	             selectMap.put("CONNECTION_HOST", host);
	             selectMap.put("TABLE_NAME", tableName);
	       
	             columnsList = access.sqlqueryWithParams(selectQuery, selectMap);
	             if (columnsList!=null && !columnsList.isEmpty()) {
	            	 columnsList = Arrays.asList(columnsList.get(0).split(","));
	             }
	             
	         } catch (Exception e) {
	             e.printStackTrace();
	         }
	         return columnsList;
	    }
            
            @Transactional
    public String insertDivIntoDB() {
    String htmlContent = "<div class=\"visionlogo\">\n" +
                 
                "</div>"+
                "<div class=\"pilog-logo\">\n" +
            "<div class=\"languageSelect\">\n" +
               " <span class=\"arabicLanguage english-text\" onclick=\"changetoArabicapp('arabic')\"\n" +
                  "  style=\"color:#00ab67;font-size: 24px; !important\">العربية</span>\n" +
                "<span class=\"englishLanguage arabic-text\" onclick=\"changetoArabicapp('english')\"\n" +
                  "  style=\"color:#0b4a99;\">English</span>\n" +
           " </div>\n" +
            
         "<span class=\"input-group-text\" id=\"basic-addon1\">\n"+
                                    " <img src=\"images/userlogin.png\" class=\"themeModeDark\" alt=\"userlogin\">\n"+
                                " </span>\n"+
                                "<span class=\"input-group-text\" id=\"basic-addon1\">\n"+
                                     "<img src=\"images/passwordicon.png\" class=\"themeModeDark\" alt=\"passwordicon\">\n"+
                                 "</span>";

        try {
           
            Connection connection = null;
             PreparedStatement statement = null;
             connection = DriverManager.getConnection(dbURL, userName, password);
            int startIndex = htmlContent.indexOf("src=\"");
            while (startIndex >= 0) {
                startIndex += 5; 
                int endIndex = htmlContent.indexOf("\"", startIndex);
                String src = htmlContent.substring(startIndex, endIndex);

              
                String[] parts = src.split("/");
                String imageName = parts[parts.length - 1];
                String imagePath = "D:\\projects\\netbeans\\October\\31-10-2023\\NIICTEST\\web\\images\\" + imageName;
                String [] fileExtentionArr =  imageName.split("\\.");
                String fileExtension =fileExtentionArr[1];
                File imageFile = new File(imagePath);
                byte[] imageBytes = new byte[(int) imageFile.length()];
                FileInputStream fileInputStream = new FileInputStream(imageFile);
                fileInputStream.read(imageBytes);
                Blob blob  = BLOB.createTemporary(connection, false, BLOB.DURATION_SESSION);
                blob.setBytes(1, imageBytes);
                String insertQuery = "INSERT INTO LG_HOME_PAGE_ATTACHMENTS (FILE_NAME, ATTACH_EXTENSION, ATTACH_TYPE, FILE_CONTENT, ACTIVE_FLAG, MESSAGE) VALUES (?, ?, ?, ?, ?, ?)";
                statement = connection.prepareStatement(insertQuery);
                  statement.setObject(1, imageName);//ORGN_ID
            statement.setObject(2, fileExtension);//USER_NAME
            statement.setObject(3, "Image");//FILE_ORG_NAME
            statement.setBlob(4, blob );//FILE_NAME
            statement.setObject(5, "Y");//FILE_PATH
            statement.setObject(6, "Sucesss");//FILE_TYPE
            int insertCount = statement.executeUpdate();
               System.out.println("insertCount::" + insertCount);
             System.out.println("Image data inserted into the database for " + src);

            fileInputStream.close();

            startIndex = htmlContent.indexOf("src=\"", endIndex);
            }
            
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    
        return "images inserted sucessfully";
        
        
    }
    
}
