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
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 *
 * @author PiLog
 */
@Repository
public class V10MigrationDataAccess {

    // connection for Oracle
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

    // connection for MYSQL
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

    public Object getSQLSERVERConnection(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName) {
        Connection connection = null;
        Object returnObj = null;
        try {

            String url = "jdbc:sqlserver://" + hostName.trim() + ":" + port.trim() + ";databaseName=" + dataBaseName.trim() + ";integratedSecurity=false;sendStringParametersAsUnicode=false;encrypt=true;trustServerCertificate=true;";
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(url, userName, password);
            returnObj = connection;
        } catch (Exception e) {
            returnObj = e.getMessage();
            e.printStackTrace();
        }
        return returnObj;
    }

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

    public Object getSAPConnection(String ClientId,
            String userName,
            String password,
            String LanguageId,
            String hostName,
            String ERPSystemId,
            String group
    ) {
        JCO.Client theConnection = null;
        Object returnObj = null;
        try {
            if (group != null && !"".equalsIgnoreCase(group) && !"null".equalsIgnoreCase(group)) {
                theConnection = JCO.createClient(
                        ClientId,
                        userName,
                        password,
                        LanguageId,
                        hostName,
                        ERPSystemId,
                        group);
                theConnection.connect();
                returnObj = theConnection;
                System.out.println(" Succesfully connect to SAP system");
            } else {
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
            }

            //System.out.println("connection attribute:"+ theConnection.getAttributes());
        } catch (Exception ex) {
            ex.printStackTrace();
            returnObj = ex.getMessage();
            System.out.println("Failed to connect to SAP system");
        }

        return returnObj;

    }
    
    public Object getSAP_ECCConnection(String ClientId,
            String userName,
            String password,
            String LanguageId,
            String hostName,
            String ERPSystemId,
            String group
    ) {
        JCO.Client theConnection = null;
        Object returnObj = null;
        try {
            if (group != null && !"".equalsIgnoreCase(group) && !"null".equalsIgnoreCase(group)) {
                theConnection = JCO.createClient(
                        ClientId,
                        userName,
                        password,
                        LanguageId,
                        hostName,
                        ERPSystemId,
                        group);
                theConnection.connect();
                returnObj = theConnection;
                System.out.println(" Succesfully connect to SAP system");
            } else {
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
            }

            //System.out.println("connection attribute:"+ theConnection.getAttributes());
        } catch (Exception ex) {
            ex.printStackTrace();
            returnObj = ex.getMessage();
            System.out.println("Failed to connect to SAP system");
        }

        return returnObj;

    }
    
    public Object getSAP_HANAConnection(String ClientId,
            String userName,
            String password,
            String LanguageId,
            String hostName,
            String ERPSystemId,
            String group
    ) {
        JCO.Client theConnection = null;
        Object returnObj = null;
        try {
            if (group != null && !"".equalsIgnoreCase(group) && !"null".equalsIgnoreCase(group)) {
                theConnection = JCO.createClient(
                        ClientId,
                        userName,
                        password,
                        LanguageId,
                        hostName,
                        ERPSystemId,
                        group);
                theConnection.connect();
                returnObj = theConnection;
                System.out.println(" Succesfully connect to SAP system");
            } else {
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
            }

            //System.out.println("connection attribute:"+ theConnection.getAttributes());
        } catch (Exception ex) {
            ex.printStackTrace();
            returnObj = ex.getMessage();
            System.out.println("Failed to connect to SAP system");
        }

        return returnObj;

    }

    public Object getSAPGroupConnection(String ClientId,
            String userName,
            String password,
            String LanguageId,
            String hostName,
            String ERPSystemId,
            String group
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
                    ERPSystemId,
                    group);
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
    
    public Object getMongoDbConnection(String hostName, String port, String userName, String password,
			String databaseName) {
		MongoClient mongoClient = null;
		Object returnObj = null;

	try {
		 // Update the connection string for local MongoDB
		String connectionString = "mongodb://" + hostName + ":" + port + "/";
        ConnectionString connString = new ConnectionString(connectionString);
        mongoClient = MongoClients.create(connString);
        //MongoDatabase database = mongoClient.getDatabase(databaseName);
        
        returnObj = mongoClient;
		} catch (Exception e) {
			returnObj = e.getMessage();
			e.printStackTrace();
		}

		return returnObj;
	}
}
