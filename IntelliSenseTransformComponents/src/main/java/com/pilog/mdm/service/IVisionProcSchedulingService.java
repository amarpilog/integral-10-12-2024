/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.pilog.mdm.DAO.IVisonProcSchedulingDAO;
import com.pilog.mdm.utilities.PilogUtilities;
import java.io.BufferedReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import oracle.sql.RAW;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.quartz.CronExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

/**
 *
 * @author Ravindar.P
 */
@Service
public class IVisionProcSchedulingService {
    
    @Autowired
    private ApplicationContext appContext;
    
    @Autowired
    private HttpServletRequest request;
    
    @Autowired
    private IVisionTransformComponentUtilities componentUtilities;
    
    @Autowired
    private IVisonProcSchedulingDAO procSchedulingDAO;
    
     Map<Object, ScheduledFuture<?>> procsMap = new HashMap<>();
    public JSONObject scheduleStoredProcedure(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            String procedureName = request.getParameter("procedureName");
            String cronExp = request.getParameter("cronExp");
            String cronStartDate = request.getParameter("cronStartDate");
            String cronEndDate = request.getParameter("cronEndDate");
            
            String enableNotifications = request.getParameter("enableNotifications");
            String notificationEmailIds = request.getParameter("notificationEmailIds");
            
            String connectionName = request.getParameter("connectionName");
            String connObjStr = request.getParameter("connObjStr");
            JSONObject connObj = (JSONObject)JSONValue.parse(connObjStr);
            String connectionUrl = connObj.get("HOST_NAME")+":"+connObj.get("CONN_PORT")+"/"+connObj.get("CONN_DB_NAME");
            
            JSONObject paramData = new JSONObject();
            
            String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            String userName = (String) request.getSession(false).getAttribute("ssUsername");
            
            paramData.put("orgnId", orgnId);
            paramData.put("userName", userName);
            paramData.put("procedureName", procedureName);
            
            paramData.put("connectionName",connectionName);
            paramData.put("connectionUrl",connectionUrl);
            paramData.put("connectionObject",connObjStr);
            
            paramData.put("cronExp", cronExp);
            paramData.put("cronStartDate", cronStartDate);
            paramData.put("cronEndDate", cronEndDate);
            
            
            resultObj = procSchedulingDAO.saveProcShedulingInfo(request, paramData);
          
            addProcedureToScheduler(request, procedureName, connObjStr, cronExp);
            
        } catch (org.hibernate.exception.ConstraintViolationException ex) {
            resultObj.put("flag", false);
            resultObj.put("message", "The Selected Job is in-process,please go the Schedule Procs tabs and check the progress.");
        } catch (Exception e) {
            resultObj.put("flag", false);
            resultObj.put("message", e.getMessage());
            e.printStackTrace();
        }
        return resultObj;
    }
    
    public void addProcedureToScheduler(HttpServletRequest request, String procedureName, String connObjStr, String cronExp) {
        try {
            String ssUsername = (String) request.getSession(false).getAttribute("ssUsername");
            String ssOrgId = (String) request.getSession(false).getAttribute("ssOrgId");
            JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
            String url = connObj.get("HOST_NAME") + ":" + connObj.get("CONN_PORT") + "/" + connObj.get("CONN_DB_NAME");
            String procId = url + "-" + procedureName + "-" + ssUsername + "-" + ssOrgId;
            Object[] procSchedularArray = procSchedulingDAO.getProcSchedulars(procedureName, url, ssOrgId, ssUsername);
            if (CronExpression.isValidExpression(cronExp)) {
                System.out.println("Valid Cron expression");
                Runnable runnableTask = () -> {
                    try {

                        executeProcedure(request, procedureName, connObjStr, procSchedularArray);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                };

                addProcedureToScheduler(procId, runnableTask, cronExp);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
     public void addProcedureToScheduler(Object procId,Runnable task,String cronExp) {
    	ThreadPoolTaskScheduler taskScheduler = (ThreadPoolTaskScheduler)appContext.getBean("ThreadPoolTaskScheduler");
        ScheduledFuture<?> scheduledTask = taskScheduler.schedule(task,
                new CronTrigger(cronExp, TimeZone.getTimeZone(TimeZone.getDefault().getID())));
                procsMap.put(procId, scheduledTask);
        
    }
     
     public void executeProcedure(HttpServletRequest request, String procedureName, String connObjStr, Object[] procSchedularArray) throws SQLException {
         SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
         Connection connection = null;
         CallableStatement cstmt = null;
         CallableStatement outputStatement = null;
         try {
             JSONObject connObj = (JSONObject) JSONValue.parse(connObjStr);
             String url = connObj.get("HOST_NAME") + ":" + connObj.get("CONN_PORT") + "/" + connObj.get("CONN_DB_NAME");
             System.out.println("Schedular Proc Execution ::" + procSchedularArray[2]);
             String cronTrigger = String.valueOf(procSchedularArray[6]);
             Date currentDate = new Date();
             Timestamp currentsqlDate=new Timestamp(currentDate.getTime());
             //Timestamp newcurrent = Timestamp.valueOf("2023-08-11 00:00:00.0");
             Date startDate = (Date) procSchedularArray[7];
             SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
             String enddate =  String.valueOf(procSchedularArray[8]);
             java.util.Date utilDate = dateFormat2.parse(enddate);
        
             java.sql.Timestamp endDate = new java.sql.Timestamp(utilDate.getTime());
//             Timestamp endDate =  (Timestamp) procSchedularArray[8];
//             
//             LocalDateTime localDateTime = endDate.toLocalDateTime();
//
//             // Add 24 hours
//             LocalDateTime newDateTime = localDateTime.plusHours(24);
//
//             // Convert back to Timestamp
//             Timestamp newEndDate = Timestamp.valueOf(newDateTime);

             String currentDateStr = dateFormat.format(currentDate);
             String startDateStr = dateFormat.format(startDate);
//             if (endDate != null) {
//                 endDate.setDate(endDate.getDate() + 1);
//             }
             if ((startDateStr != null
                     && !"".equalsIgnoreCase(startDateStr)
                     && (startDateStr.equalsIgnoreCase(currentDateStr)
                     || currentDate.after(startDate)))
                     && (endDate != null && currentsqlDate.before(endDate))) {
                 if (procSchedulingDAO.checkProcRunningStatus(procSchedularArray)) {
                     try {
                         procSchedulingDAO.updateProcRunningStatus(procSchedularArray, "RUNNING");
                     } catch (Exception e) {
                     }

                     try {
                         java.sql.Date procStartDateSql = new java.sql.Date(new java.util.Date().getTime());
                         procSchedulingDAO.updateProcStartDate(procedureName, url, procStartDateSql);
                     } catch (Exception e) {
                         e.printStackTrace();
                     }
                      try {
	            	
                        java.sql.Date lastrunsqlDate = new java.sql.Date(new java.util.Date().getTime());
	            	procSchedulingDAO.updateProcLastRunningDate(procedureName, url, lastrunsqlDate);
	            } catch (Exception e) {
                        
	            }
                     Date procStartDate = new Date();
                     try {
                         Date nextValidTime = new CronExpression(cronTrigger).getNextValidTimeAfter(new Date());
                         java.sql.Date sqlDate = new java.sql.Date(nextValidTime.getTime());
                         procSchedulingDAO.updateProcNextRunningDate(procedureName, url, sqlDate);
                     } catch (Exception e) {
                         e.printStackTrace();
                     }

                 }
                 connection = (Connection) componentUtilities.getConnection(connObj);

                 Statement enableStmt = connection.createStatement();
                 enableStmt.executeUpdate("BEGIN DBMS_OUTPUT.ENABLE(NULL); END;");
                 enableStmt.close();

                 cstmt = connection.prepareCall("CALL " + procedureName + "()");
                 cstmt.execute();
                 System.out.println("proc is executing");

                 outputStatement = connection.prepareCall("BEGIN DBMS_OUTPUT.GET_LINES(?, ?); END;");
                 outputStatement.registerOutParameter(1, Types.ARRAY, "DBMSOUTPUT_LINESARRAY");
                 outputStatement.registerOutParameter(2, Types.INTEGER);
                 outputStatement.execute();

                 Array array = outputStatement.getArray(1);
                 String[] lines = (String[]) array.getArray();
//                 String outputlines = String.join(" ", lines); 
                
                 String outputlines = "";
                 for (String line : lines) {
                     if (line != null) {
                         System.out.println(line);
                         outputlines += "$" + line;
                     }
                 }
                 DateFormat dfor = new SimpleDateFormat("dd-MM-yy HH:mm:ss");
                Calendar obj = Calendar.getInstance();
                String outputDateTime = dfor.format(obj.getTime());
                  procSchedulingDAO.procDbmsOutput(procSchedularArray, outputlines, outputDateTime);
                 
             } else if ((endDate != null && (currentsqlDate.after(endDate) || currentsqlDate.equals(endDate)))
                     && procSchedulingDAO.checkProcRunningStatus(procSchedularArray)) {
                 removeProcFromScheduler(procSchedularArray[4] + "-" + procSchedularArray[2] + "-" + procSchedularArray[1] + "-" + procSchedularArray[0]);
                 try {
                     int updateCount = procSchedulingDAO.updateProcActiveStatus((String) procSchedularArray[2], url,
                             (String) procSchedularArray[0],
                             (String) procSchedularArray[1],
                             "N","STOPPED");
                 } catch (Exception e) {
                 }
             }

         } catch (Exception e) {
             e.printStackTrace();
         } finally {
        	 if(outputStatement != null) {
        		 outputStatement.close();
        	 }
        	 if(cstmt != null) {
        		 cstmt.close();
        	 }
//        	 if(connection != null) {
//        		 connection.close();
//        	 }

         }
    }
     
     public JSONObject getScheduledProcs(HttpServletRequest request) {
         JSONObject scheduledProcsObj = new JSONObject();
        try {
            JSONArray procScheduledArray = new JSONArray();
            JSONObject runningProcsItem = new JSONObject();
            runningProcsItem.put("id", "RUNNING_PROCS");
            runningProcsItem.put("value", "RUNNING_PROCS");
            runningProcsItem.put("text", "RUNNING_PROCS");
            procScheduledArray.add(runningProcsItem);
            
            JSONObject resultObj = procSchedulingDAO.getScheduledProcs(request);
            List<Object[]> scheduledProcsList = (List<Object[]>)resultObj.get("procsList");
            JSONObject tableObj = new JSONObject();
            for (Object[] scheduledProcArray : scheduledProcsList) {
                 tableObj = new JSONObject();
                 tableObj.put("id", scheduledProcArray[2]);
                 tableObj.put("text", scheduledProcArray[2]);
                 tableObj.put("value",scheduledProcArray[5]);
                 tableObj.put("parentid","RUNNING_PROCS");
                 procScheduledArray.add(tableObj);
            }
            scheduledProcsObj.put("procScheduledArray", procScheduledArray);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scheduledProcsObj;
    }
     
     // Remove scheduled proc 
    public void removeProcFromScheduler(Object procId) {
        ScheduledFuture<?> scheduledTask = procsMap.get(procId);
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
            //jobsMap.remove(id);
            procsMap.put(procId, null);
        }
    }
    
    public JSONObject processScheduledProc(HttpServletRequest request) {
        JSONObject scheduledProcObj = new JSONObject();
        try {
            String procedureName = request.getParameter("procName");
            String connObjStr = request.getParameter("connectionObjStr");
            String ssUsername = (String)request.getSession(false).getAttribute("ssUsername");
            String ssOrgId = (String)request.getSession(false).getAttribute("ssOrgId");
            JSONObject connObj = (JSONObject)JSONValue.parse(connObjStr);
            String url = connObj.get("HOST_NAME")+":"+connObj.get("CONN_PORT")+"/"+connObj.get("CONN_DB_NAME");
            String procId = url+"-"+procedureName+"-"+ssUsername+"-"+ssOrgId;
            String message = "";
            String procFlag =  request.getParameter("procFlag");
            if ("STOP".equalsIgnoreCase(procFlag)) {
                removeProcFromScheduler(procId);
                if(procsMap.get("procId")==null){
                    int updateCount = procSchedulingDAO.updateProcActiveStatus(procedureName,url,ssOrgId,ssUsername,
                        "N","STOPPED"      
                );
                   if (updateCount != 0) {
                       message = "Selected Proc has been stoped successfully.";
                       
                   }else{
                       message = "Selected proc failed to stop.";
                   }
                }
                
               
            }else if ("REMOVE".equalsIgnoreCase(procFlag)) {
                removeProcFromScheduler(procId);
                if(procsMap.get("procId")==null){
                    int deleteCount = procSchedulingDAO.deleteScheduledProc(procedureName,url,ssOrgId,ssUsername);
                    if (deleteCount != 0) {
                        message = "Selected Proc schedular has been removed successfully .";
                        
                    }else{
                        message = "Failed to remove the proc schedular";
                    }
                }
                
            }
            scheduledProcObj.put("message",message);
            scheduledProcObj.put("flag", true);
        } catch (Exception e) {
            scheduledProcObj.put("message", e.getMessage());
            scheduledProcObj.put("flag", false);
            e.printStackTrace();
        }
        return scheduledProcObj;
    }
    
    // A context refresh event listener
    @EventListener({ContextRefreshedEvent.class})
    void contextRefreshedEvent() {
        try {
            procsMap = new HashMap<>();
            List<Object[]> procSchedularsList = procSchedulingDAO.getActiveScheduledProcs();
            if (procSchedularsList != null && !procSchedularsList.isEmpty()) {
                for (Object[] procSchedularsArray : procSchedularsList) {
                    if (procSchedularsArray != null
                            && CronExpression.isValidExpression(String.valueOf(procSchedularsArray[6]))) {
                        Runnable runnableTask = () -> {
                            try {
                                executeProcedure(request, String.valueOf(procSchedularsArray[2]), String.valueOf(procSchedularsArray[5]), procSchedularsArray);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        };
                        procSchedularsArray[0] = new RAW((byte[]) procSchedularsArray[0]).stringValue();
                        addProcedureToScheduler(procSchedularsArray[4] + "-" + procSchedularsArray[2] + "-" + procSchedularsArray[1] + "-" + procSchedularsArray[0], runnableTask, (String)procSchedularsArray[6]);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Get all tasks from DB and reschedule them in case of context restarted
    }
    
    public JSONObject fetchRemoveProcFromScheduler(HttpServletRequest request) {
         JSONObject resultObj = new JSONObject();
        try {
            String procedureName = request.getParameter("procName");
            String connObjStr = request.getParameter("connObjStr");
            String ssUsername = (String)request.getSession(false).getAttribute("ssUsername");
            String ssOrgId = (String)request.getSession(false).getAttribute("ssOrgId");
            JSONObject connObj = (JSONObject)JSONValue.parse(connObjStr);
            String url = connObj.get("HOST_NAME")+":"+connObj.get("CONN_PORT")+"/"+connObj.get("CONN_DB_NAME");
            String procId = url+"-"+procedureName+"-"+ssUsername+"-"+ssOrgId;
            String message = "";
            if(procsMap.containsKey(procId)){
                    int updateCount = procSchedulingDAO.fetchRemoveProcFromScheduler(procedureName,url,ssOrgId,ssUsername);
                   if (updateCount != 0) {
                       message = "Selected Proc has been Running. Are you sure to Remove!";
                       
                   }else{
                       message = "Selected Proc has been Scheduled. Are you sure to Remove!";
                   }
                }else{
                        message = "Are you sure to Remove Selected Proc!";
                }
            resultObj.put("message",message);
            resultObj.put("flag", true);
        } catch (Exception e) {
            resultObj.put("message", e.getMessage());
            resultObj.put("flag", false);
            e.printStackTrace();
        }
        return resultObj;
    }
    
    public Map fetchProcScheduleInfo(HttpServletRequest request) {
        Map scheduledJobObj = new LinkedHashMap();
        try {
            String procedureName = request.getParameter("procName");
            String connObjStr = request.getParameter("connectionObjStr");
            String ssUsername = (String)request.getSession(false).getAttribute("ssUsername");
            String ssOrgId = (String)request.getSession(false).getAttribute("ssOrgId");
            JSONObject connObj = (JSONObject)JSONValue.parse(connObjStr);
            String url = connObj.get("HOST_NAME")+":"+connObj.get("CONN_PORT")+"/"+connObj.get("CONN_DB_NAME");
        	scheduledJobObj = procSchedulingDAO.fetchProcScheduleInfo(procedureName,url,ssOrgId,ssUsername);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scheduledJobObj;
    }
    
    public Map fetchProcScheduleLog(HttpServletRequest request) {
        Map scheduledJobObj = new LinkedHashMap();
        try {
            String procedureName = request.getParameter("procName");
            String connObjStr = request.getParameter("connectionObjStr");
            String ssUsername = (String)request.getSession(false).getAttribute("ssUsername");
            String ssOrgId = (String)request.getSession(false).getAttribute("ssOrgId");
            JSONObject connObj = (JSONObject)JSONValue.parse(connObjStr);
            String url = connObj.get("HOST_NAME")+":"+connObj.get("CONN_PORT")+"/"+connObj.get("CONN_DB_NAME");
        	scheduledJobObj = procSchedulingDAO.fetchProcScheduleLog(procedureName,url,ssOrgId,ssUsername);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scheduledJobObj;
    }
    
}
