/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.transformaccess;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.springframework.stereotype.Repository;

/**
 *
 * @author Devint01
 */
@Repository
public class iVisioniTransformCompHibAccess {

    // method for getting Hibernate SessionFactory
    public SessionFactory getSessionFactoryObject(String hostName,
            String port,
            String userName,
            String password,
            String dataBaseName,
            String dbType) {
        SessionFactory sessionFactory = null;
        try {
            String driverName = "";
            String dialectName = "";
            String dbURL = "";
            if ("ORACLE".equalsIgnoreCase(dbType) || "Oracle_ERP".equalsIgnoreCase(dbType)) {
                driverName = "oracle.jdbc.driver.OracleDriver";
                dialectName = "org.hibernate.dialect.OracleDialect";
                dbURL = "jdbc:oracle:thin:@//" + hostName.trim() + ":" + port.trim() + "/" + dataBaseName.trim();
            } else if ("MYSQL".equalsIgnoreCase(dbType)) {
                driverName = "com.mysql.jdbc.Driver";
                dialectName = "org.hibernate.dialect.MySQLDialect";
                dbURL = "jdbc:mysql://" + hostName.trim() + ":" + port.trim() + "/" + dataBaseName.trim() + "?characterEncoding=latin1";
            } else if ("SQLSERVER".equalsIgnoreCase(dbType)) {
                driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                dialectName = "org.hibernate.dialect.SQLServer2012Dialect";
                dbURL = "jdbc:sqlserver://" + hostName.trim() + ":" + port.trim() + ";databaseName=" + dataBaseName.trim() + ";integratedSecurity=false;sendStringParametersAsUnicode=false;";
            } else if ("DB2".equalsIgnoreCase(dbType)) {
            } else if ("PostgreSQL".equalsIgnoreCase(dbType)) {
                //jdbc:postgresql://localhost:5432/phonesdemo
                driverName = "org.postgresql.Driver";
                dialectName = "org.hibernate.dialect.PostgreSQL92Dialect";
                dbURL = "jdbc:postgresql://" + hostName.trim() + ":" + port.trim() + "/" + dataBaseName.trim();
            }
            Configuration config = new Configuration();
            config.setProperty("hibernate.connection.driver_class", driverName);
            config.setProperty("hibernate.connection.url", dbURL);
            config.setProperty("hibernate.connection.username", userName);
            config.setProperty("hibernate.connection.password", password);
            config.setProperty("hibernate.dialect", dialectName);
            config.setProperty("hibernate.use_sql_comments", "false");
            config.setProperty("hibernate.generate_statistics", "false");
            config.setProperty("hibernate.show_sql", "false");
            sessionFactory = config.buildSessionFactory();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sessionFactory;
    }

    // method for fetching DB tables data using Hibernate with rows limit
    @SuppressWarnings("rawtypes")
    public List queryWithParamsWithLimit(String query,
            Session session,
            SessionFactory sessionFactory,
            int limit,
            int start)
            throws Exception {
        // Session session = null;
        List list = null;
        try {
            if (start > 0) {
                start = start - 1;
            }
            session = sessionFactory.openSession();
//             session = sessionFactory.getCurrentSession();
            SQLQuery queryObj = session.createSQLQuery(query)
                    .setMaxResults(limit)
                    .setFirstResult(start);
            list = queryObj.list();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        } finally {
            try {
                if (session != null) {
//                     session.flush();
                    session.close();
                    session = null;
                    System.gc();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    // method for fetching DB tables data using Hibernate with rows limit
    @SuppressWarnings("rawtypes")
    public List queryWithParamsNoLimits(String query,
            Session session,
            SessionFactory sessionFactory)
            throws Exception {
        // Session session = null;
        List list = null;
        try {

            session = sessionFactory.openSession();
//             session = sessionFactory.getCurrentSession();
            SQLQuery queryObj = session.createSQLQuery(query);
            list = queryObj.list();
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            try {
                if (session != null) {
//                     session.flush();
                    session.close();
                    session = null;
                    System.gc();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }
}
