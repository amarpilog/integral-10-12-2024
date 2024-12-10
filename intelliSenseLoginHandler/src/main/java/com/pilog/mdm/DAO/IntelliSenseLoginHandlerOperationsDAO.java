/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;

import com.pilog.mdm.utilities.PilogUtilities;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.pojo.SPersAudit;
import com.pilog.mdm.pojo.SPersAuditId;
import com.pilog.mdm.pojo.SPersDetail;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author Devint01
 */
@Repository
public class IntelliSenseLoginHandlerOperationsDAO {
    
    @Autowired
    private DataAccess access;
    
    @Value("${command.path}")
    private String macCommand;
    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.sss");
    private PilogUtilities cloudUtills = new PilogUtilities();
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void loginParams(SPersDetail persDetail, HttpServletRequest request,JSONObject sessionObj) {
        System.out.println("loginParams:Start:::" + sdf.format(new Date()));
        try {
            if (persDetail != null) {
            	String userTimeZone=request.getParameter("timezone");
                SPersAudit audit = new SPersAudit();
                SPersAuditId auditId = new SPersAuditId();
                //auditId.setSessionId(request.getParameter("sessionId"));
                auditId.setPersId(persDetail.getPersId());
                ZoneId zoneId = ZoneId.of(userTimeZone);
                ZonedDateTime userLocalTime = ZonedDateTime.now(zoneId);
                audit.setLoginDate(Timestamp.from(userLocalTime.toInstant()));
               // audit.setLoginDate(cloudUtills.getCurrentTimeZoneData((String) sessionObj.get("ssTimeZone")));
                audit.setFlag("N");
                HttpSession loginSession = request.getSession();
                System.out.println("loginSession:::" + loginSession.getId());
                auditId.setSessionId(loginSession.getId());
                String deviceDetailsStr = new PilogUtilities().getDeviceDetails(request, "");
                //String DEVICEDETAILS = request.getParameter("DEVICE_DETAILS");
                JSONObject devicedetailsObj = (JSONObject) JSONValue.parse(deviceDetailsStr);
                audit.setBrowser((String) devicedetailsObj.get("browserName"));
                audit.setIpAddress((String) devicedetailsObj.get("IPAddress"));
                audit.setDeviceName((String) devicedetailsObj.get("computerName"));
                audit.setCreateBy(persDetail.getUserName().toUpperCase());
                audit.setCreateDate(new Timestamp(new Date().getTime()));
                audit.setEditBy(persDetail.getUserName().toUpperCase());
                audit.setEditDate(new Timestamp(new Date().getTime()));
                audit.setAuditId(AuditIdGenerator.getAuditId("S_PERS_AUDIT"));
                audit.setId(auditId);
                access.saveObj(audit);
            }

        } catch (Exception ex) {
            ex.printStackTrace();

        }
        System.out.println("loginParams:End:::" + sdf.format(new Date()));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateLOginFailureAttempts(String persId, int loginAttempts, boolean failureFlag) {

        try {
            // String query = " from SPersonnel where persId =:persId";

            String updateQuery = "UPDATE S_PERSONNEL SET LOGIN_ATTEMPTS =:LOGIN_ATTEMPTS WHERE PERS_ID  =:PERS_ID";
//            String updateQuery = "update SPersonnel set loginAttempts =:loginAttempts where persId  =:persId";
            Map<String, Object> updateMap = new HashMap<>();
            if (persId != null) {
                loginAttempts++;
                if (failureFlag) {
                    loginAttempts = 0;
                }
                updateMap.put("LOGIN_ATTEMPTS", loginAttempts);
                updateMap.put("PERS_ID", persId);
//                updateMap.put("loginAttempts", new BigInteger(String.valueOf(loginAttempts)));
//                updateMap.put("persId", personnel.getPersId());
                System.out.println("updateQuery:::" + updateQuery);
                System.out.println("updateMap:::" + updateMap);
                int updateCount = access.executeUpdateSQL(updateQuery, updateMap);
//                int updateCount = access.updateQueryByCountNoAudit(updateQuery, updateMap);
                System.out.println("updateCount:::" + updateCount);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateLOginFailureAttempts(String userName, boolean failureFlag) {
        try {
            int loginAttempts = 0;
            if (!failureFlag) {
                String selectQuery = "SELECT LOGIN_ATTEMPTS FROM S_PERSONNEL "
                        + "WHERE PERS_ID =(SELECT PERS_ID FROM S_PERS_DETAIL WHERE USER_NAME='" + userName + "')";
                List loginAttemptsList = access.sqlqueryWithParams(selectQuery, Collections.EMPTY_MAP);
                if (loginAttemptsList != null && !loginAttemptsList.isEmpty()) {
                    loginAttempts = cloudUtills.convertIntoInteger(loginAttemptsList.get(0));
                    loginAttempts++;
                }
            }
            String updateQuery = "UPDATE S_PERSONNEL SET LOGIN_ATTEMPTS ='" + loginAttempts + "' "
                    + "WHERE PERS_ID =(SELECT PERS_ID FROM S_PERS_DETAIL WHERE USER_NAME='" + userName + "')";
            System.out.println("updateQuery:::" + updateQuery);
            int updateCount = access.executeUpdateSQL(updateQuery, Collections.EMPTY_MAP);
//                int updateCount = access.updateQueryByCountNoAudit(updateQuery, updateMap);
            System.out.println("updateCount:::" + updateCount);
        } catch (Exception e) {
        }

    }

    @Transactional
    public String replaceingSessionValue(String sessionName, String query, JSONObject session) {

        if (query != null && query.contains(sessionName)) {
            String sessionValue = "'" + session.get(sessionName) + "'";
            query = query.replaceAll(sessionName, sessionValue);

        }
        return query;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateLogOutByAllUsername(String userName, HttpServletRequest request) {
        try {
            SPersDetail details = getPersDetailsByUsername(userName.toUpperCase());
            if (details != null) {
//                updateLogOutByAllPersId(details.getPersId(), request);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int updateLogOutByAllPersId(String persId, String sessionId) {
        int updateCount = 0;
        try {
            String updateQuery = "UPDATE S_PERS_AUDIT SET FLAG =:FLAG ,LOGOUT_DATE=:LOGOUT_DATE WHERE PERS_ID =:PERS_ID AND "
                    + " SESSION_ID <> :SESSION_ID AND FLAG ='N' ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("FLAG", "Y");
            updateMap.put("LOGOUT_DATE", new Date());
            updateMap.put("PERS_ID", persId);
            updateMap.put("SESSION_ID", sessionId);
            updateCount = access.executeUpdateSQL(updateQuery, updateMap);
            System.out.println("updateCount::updateLogOutByAllPersId::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int updateLogOutByAllPersId(String persId, HttpServletRequest request) {
        int updateCount = 0;
        try {
        	 String sessionId = request.getSession(false).getId();
            String updateQuery = "UPDATE S_PERS_AUDIT SET FLAG =:FLAG ,LOGOUT_DATE=:LOGOUT_DATE WHERE PERS_ID =:PERS_ID AND "
                    + " SESSION_ID <> :SESSION_ID AND FLAG ='N' ";
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("FLAG", "Y");
            updateMap.put("LOGOUT_DATE", new Date());
            updateMap.put("PERS_ID", persId);
            updateMap.put("SESSION_ID", sessionId);
            updateCount = access.executeUpdateSQL(updateQuery, updateMap);
            System.out.println("updateCount::updateLogOutByAllPersId::" + updateCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updateCount;
    }

    @Transactional
    public List getAuditHistByPersId(String persId, String currentSessionId) {
        List list = null;

        try {

            String query = "from SPersAudit where flag='N' and id.persId=:persId and id.sessionId <> :sessionId order by createDate desc";
            Map<String, Object> map = new HashMap<String, Object>();

//            map.put("loginDate", new Timestamp(new Date().getTime()));
            map.put("persId", persId);
            map.put("sessionId", currentSessionId);
            list = access.sqlqueryWithParams(query, map);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;

    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public SPersDetail getPersDetailsByUsername(String username) {
        SPersDetail detail = null;
        try {

            String query = "from SPersDetail where userName=:userName";
            Map<String, Object> map = new HashMap<>();
            map.put("userName", username.toUpperCase());
            List list = access.queryWithParams(query, map);

            if (list != null && !list.isEmpty()) {
                detail = (SPersDetail) list.get(0);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return detail;

    }
    
    
}
