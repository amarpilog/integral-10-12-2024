/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.transformaccess;

import com.sap.mw.jco.JCO;
import java.sql.Connection;
import java.sql.DriverManager;
import org.springframework.stereotype.Repository;

/**
 *
 * @author PiLog
 */
@Repository
public class iVisioniTransformCompConnections {

    // get connection for Oracle DB
    public Object getOracleConnection(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName) {
        Connection connection = null;
        String errorMsg = "";
        Object returnObj = null;
        try {
            String url = "jdbc:oracle:thin:@//" + hostName.trim() + ":" + port.trim() + "/" + dataBaseName.trim();
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;
        } catch (Exception e) {
            errorMsg = "Unable to make connection with DB";
            returnObj = e.getMessage();
            e.printStackTrace();
        }
        return returnObj;
    }

    // get connection for MYSQL DB
    public Object getMYSQLConnection(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName) {
        Connection connection = null;
        Object returnObj = null;
        try {
            String url = "jdbc:mysql://" + hostName.trim() + ":" + port.trim() + "/" + dataBaseName.trim() + "?characterEncoding=latin1&maxActive=600000";
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;

        } catch (Exception e) {
            returnObj = e.getMessage();
            e.printStackTrace();
        }
        return returnObj;
    }

    // get connection for MSSQL DB
    public Object getSQLSERVERConnection(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName) {
        Connection connection = null;
        Object returnObj = null;
        try {

            String url = "jdbc:sqlserver://" + hostName.trim() + ":" + port.trim() + ";databaseName=" + dataBaseName.trim() + ";integratedSecurity=false;sendStringParametersAsUnicode=false;";
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;
        } catch (Exception e) {
            returnObj = e.getMessage();
            e.printStackTrace();
        }
        return returnObj;
    }

     // get connection for MSAccess DB
    public Object getMSAccessConnection(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName) {
        Connection connection = null;
        Object returnObj = null;
        try {
            String fileName = "";
            String url = "jdbc:odbc:Driver={Microsoft Access Driver (*.mdb)};" + "DBQ=" + fileName;
            //"jdbc:odbc:Driver={Microsoft Access Driver (*.mdb)};DBQ="+fileName
            Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;
        } catch (Exception e) {
            returnObj = e.getMessage();
            e.printStackTrace();
        }
        return returnObj;
    }

    // get connection for SQLite DB
    public Object getSQLiteConnection(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName) {
        Connection connection = null;
        Object returnObj = null;
        try {
            String fileName = "";
            String url = "jdbc:sqlite:C:/sqlite/" + fileName;
            //jdbc:sqlite:C:/sqlite/JTP.db
            Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;
        } catch (Exception e) {
            returnObj = e.getMessage();
            e.printStackTrace();
        }
        return returnObj;
    }

     // get connection for PostgreSQL DB
    public Object getPostgreSQLConnection(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName) {
        Connection connection = null;
        Object returnObj = null;
        try {

            String url = "jdbc:postgresql://" + hostName.trim() + ":" + port.trim() + "/" + dataBaseName.trim();
            //jdbc:postgresql:// server-name : server-port / database-name Note – NOTE: Default server port is 5432.
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;
        } catch (Exception e) {
            returnObj = e.getMessage();
            e.printStackTrace();
        }
        return returnObj;
    }
    
    
 // get connection for SAP 
    public Object getSAP_HANAConnection(String ClientId,
            String userName,
            String password,
            String LanguageId,
            String hostName,
            String ERPSystemId
    ) {
        JCO.Client theConnection = null;
        Object returnObj = null;
        try {
            theConnection = JCO.createClient(
                    ClientId,
                    userName,
                    password,
                    LanguageId,
                    hostName,
                    ERPSystemId);
            theConnection.connect();
            returnObj = theConnection;
            System.out.println(" Succesfully connect to SAP system");
            //System.out.println("connection attribute:"+ theConnection.getAttributes());
        } catch (Exception ex) {
            ex.printStackTrace();
           returnObj = ex.getMessage();
            System.out.println("Failed to connect to SAP system");
        }

        return returnObj;

    }
    
    
 // get connection for SAP 
    public Object getSAP_ECCConnection(String ClientId,
            String userName,
            String password,
            String LanguageId,
            String hostName,
            String ERPSystemId
    ) {
        JCO.Client theConnection = null;
        Object returnObj = null;
        try {
            theConnection = JCO.createClient(
                    ClientId,
                    userName,
                    password,
                    LanguageId,
                    hostName,
                    ERPSystemId);
            theConnection.connect();
            returnObj = theConnection;
            System.out.println(" Succesfully connect to SAP system");
            //System.out.println("connection attribute:"+ theConnection.getAttributes());
        } catch (Exception ex) {
            ex.printStackTrace();
           returnObj = ex.getMessage();
            System.out.println("Failed to connect to SAP system");
        }

        return returnObj;

    }
    
    // get connection for SAP 
    public Object getSAPConnection(String ClientId,
            String userName,
            String password,
            String LanguageId,
            String hostName,
            String ERPSystemId
    ) {
        JCO.Client theConnection = null;
        Object returnObj = null;
        try {
            theConnection = JCO.createClient(
                    ClientId,
                    userName,
                    password,
                    LanguageId,
                    hostName,
                    ERPSystemId);
            theConnection.connect();
            returnObj = theConnection;
            System.out.println(" Succesfully connect to SAP system");
            //System.out.println("connection attribute:"+ theConnection.getAttributes());
        } catch (Exception ex) {
            ex.printStackTrace();
           returnObj = ex.getMessage();
            System.out.println("Failed to connect to SAP system");
        }

        return returnObj;

    }

    
        // get connection for Oracle ERP 
        public Object getOracle_ERPConnection(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName) {
        Connection connection = null;
        Object returnObj = null;
        try {
            String url = "jdbc:oracle:thin:@//" + hostName.trim() + ":" + port.trim() + "/" + dataBaseName.trim();
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return returnObj;
    }
    
    // get connection for Oracle ERP     
    public Object getOracle_ERPConnection(String port,
            String userName,
            String password,
            String languageId,
            String hostName,
            String dataBaseName) {
        Connection connection = null;
        Object returnObj = null;
        try {
            String url = "jdbc:oracle:thin:@//" + hostName.trim() + ":" + port.trim() + "/" + dataBaseName.trim();
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return returnObj;
//                    String.valueOf(dbObj.get("ClientId")), String.valueOf(dbObj.get("userName")),
//                    String.valueOf(dbObj.get("password")), String.valueOf(dbObj.get("LanguageId")), String.valueOf(dbObj.get("hostName")),
//                    String.valueOf(dbObj.get("ERPSystemId"))
    }
}
