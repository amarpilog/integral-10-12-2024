/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;


import com.pilog.mdm.DAO.V10JobSchedulingProcessDAO;
import com.pilog.mdm.transformcomputilities.NotificationUtills;
import com.pilog.mdm.utilities.PilogUtilities;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ScheduledFuture;
import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.quartz.CronExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
//import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

/**
 *
 * @author sanya
 */
@Service
public class V10JobSchedulingProcessService {

    @Autowired
    private V10JobSchedulingProcessDAO jobSchedulingProcessDAO;
 
    
    @Autowired
    private HttpServletRequest request;
	/*
	 * @Autowired
	 * 
	 * @Qualifier private ThreadPoolTaskScheduler taskScheduler;
	 */
    
    @Autowired
    private ApplicationContext appContext;
    
    @Autowired
    private IVisionTransformProcessJobComponentsService processJobComponentsService;
    
	@Autowired
	IVisionTransformComponentUtilities componentUtilities;
    
	@Autowired
	NotificationUtills notificationUtills;
	
    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;
    
   
    
    public void executeJob(HttpServletRequest request ,Object[] jobSchedularArray) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
        try {
            System.out.println("Schedular Job Execution ::"+jobSchedularArray[3]);
            if (jobSchedularArray != null 
                    && jobSchedularArray.length != 0) {                // need to update RUNNING_STATUS 
            	String jobId = String.valueOf(jobSchedularArray[2]);
            	String cronTrigger = String.valueOf(jobSchedularArray[4]);
                Date currentDate = new Date();
                Date startDate = (Date) jobSchedularArray[24];
                Date endDate = (Date) jobSchedularArray[25];
                String currentDateStr = dateFormat.format(currentDate);
                String startDateStr = dateFormat.format(startDate);
                if (endDate != null) {
                    endDate.setDate(endDate.getDate() + 1);
                }
//                System.out.println("currentDateStr:::"+currentDateStr);
//                System.out.println("startDateStr:::"+startDateStr);
////                System.out.println("endDate:::"+endDate);
//                System.out.println("startDate:::"+startDate.after(currentDate));
//                System.out.println("startDate:::"+startDate.before(currentDate));
//                System.out.println("endDate:::"+(!currentDate.after(startDate) && !currentDate.before(startDate)));
                if ((startDateStr != null &&
                        !"".equalsIgnoreCase(startDateStr)
                        && (startDateStr.equalsIgnoreCase(currentDateStr)
                        ||currentDate.after(startDate)))
                        && ((endDate != null && !currentDate.after(endDate)) || endDate == null)) {
                    if (jobSchedulingProcessDAO.checkRunningStatus(jobSchedularArray)) {
                        try {
                            jobSchedulingProcessDAO.updateRunningStatus(jobSchedularArray, "RUNNING");
                        } catch (Exception e) {
                        }
                        
                        try {
                        	java.sql.Date jobStartDateSql = new java.sql.Date(new java.util.Date().getTime());
                            jobSchedulingProcessDAO.updateJobStartDate(jobId, jobStartDateSql);
                        } catch (Exception e) {
                        }
                        Date jobStartDate = new Date();
                        try {
                        	 Date nextValidTime = new CronExpression(cronTrigger).getNextValidTimeAfter(new Date());
                        	 java.sql.Date sqlDate = new java.sql.Date(nextValidTime.getTime());
                            jobSchedulingProcessDAO.updateNextRunningDate(jobId, sqlDate);
                        } catch (Exception e) {
                        	e.printStackTrace();
                        }
                        
                        List<Object[]> selectedJobsList = jobSchedulingProcessDAO.getJobsListByJobId(jobSchedularArray[2],
                                jobSchedularArray[0]);
                        if (selectedJobsList != null
                                && !selectedJobsList.isEmpty()) {
                            for (Object[] selectedJobsArray : selectedJobsList) {
                                if (selectedJobsArray != null && selectedJobsArray.length != 0) {
                                    String orgnId = (String) selectedJobsArray[0];  // --------ravi scheduling code
                                    //String jobId = (String) selectedJobsArray[2];
                                    
                                    String flowChartDataStr = "";
                                    String processJobDataStr = "";
                                    
                                    if (selectedJobsArray[6] instanceof Clob) {
                                    	 flowChartDataStr = new PilogUtilities().clobToString((Clob) selectedJobsArray[6]);
                                    } else {
                                    	flowChartDataStr = String.valueOf(selectedJobsArray[6]);
                                    }
                                    
                                    if (selectedJobsArray[7] instanceof Clob) {
                                    	 processJobDataStr = new PilogUtilities().clobToString((Clob) selectedJobsArray[7]);
                                    } else {
                                    	 processJobDataStr = String.valueOf(selectedJobsArray[7]);
                                    }
                                    
                                    JSONObject flowchartData = (JSONObject) JSONValue.parse(flowChartDataStr);
//                                    dataTransformationService.processJobData(jobId, 
//                                            orgnId, 
//                                            (String) selectedJobsArray[4], 
//                                            processJobDataStr, 
//                                            flowChartDataStr,"");
                                  //  HttpServletRequest request = ((ServletRequestAttributes)RequestContextHolder.currentRequestAttributes()).getRequest();
                                    //processJobComponents(request, flowchartData,  jobId);
                                    
//                                    try {
//                                    	String  processClass = "com.pilog.mdm.service.IVisionTransformProcessJobComponentsService";
//                                    	String processMethod = "processJobComponents";
//                                    	Class clazz = Class.forName(processClass);
//                                        Class<?>[] paramTypes = {HttpServletRequest.class, JSONObject.class, String.class};
//                                        Method method = clazz.getMethod(processMethod.trim(), paramTypes);
//                                        Object targetObj = new PilogUtilities().createObjectByName(processClass);
//                                        method.invoke(targetObj, request, flowchartData, jobId);
//                                    }  catch (Exception ex) {
//                                        ex.printStackTrace();
//                                    }
                                    
                                    flowchartData.put("scheduleJob", "Y");
                                    flowchartData.put("scheduleJobStartTime", jobStartDate);
                                    flowchartData.put("enableNotifications", request.getParameter("enableNotifications"));
                                    flowchartData.put("notificationEmailIds", request.getParameter("notificationEmailIds"));
                                    processJobComponentsService.processJobComponents(request, flowchartData, jobId);
                                                                        
                                }
                            }
                        }
                        
                    }  
                }else if (endDate != null && currentDate.after(endDate) 
                        && jobSchedulingProcessDAO.checkRunningStatus(jobSchedularArray)) {
                    removeTaskFromScheduler(jobSchedularArray[2]+""+jobSchedularArray[0]+""+jobSchedularArray[1]);
                    try {
                        int updateCount = jobSchedulingProcessDAO.updateActiveStatus((String) jobSchedularArray[0],
                              (String) jobSchedularArray[2],
                                (String) jobSchedularArray[1],
                                "N"
                        );
                    } catch (Exception e) {
                    }
                }
                
               
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Schedule Task to be executed every night at 00 or 12 am
    public void addTaskToScheduler(Object id, Runnable task,String cronExp) {
    	ThreadPoolTaskScheduler taskScheduler = (ThreadPoolTaskScheduler)appContext.getBean("ThreadPoolTaskScheduler");
        ScheduledFuture<?> scheduledTask = taskScheduler.schedule(task,
                new CronTrigger(cronExp, TimeZone.getTimeZone(TimeZone.getDefault().getID())));
        jobsMap.put(id, scheduledTask);
        
    }
    
    public void addTaskToScheduler(HttpServletRequest request, String jobId,Object orgnId,String userName) {
        try {
            Object[] jobSchedularsArray = jobSchedulingProcessDAO.getJobSchedulars(jobId,orgnId,userName);
            if (jobSchedularsArray != null
                    && jobSchedularsArray.length != 0
                    && jobSchedularsArray[4] != null
                    && !"".equalsIgnoreCase(String.valueOf(jobSchedularsArray[4]))
                    && !"null".equalsIgnoreCase(String.valueOf(jobSchedularsArray[4]))
                    && CronExpression.isValidExpression(String.valueOf(jobSchedularsArray[4]))) {
            	System.out.println("Valid Cron expression");
                Runnable runnableTask = () -> {
                    try {
                    	
                        executeJob(request, jobSchedularsArray);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                };
                addTaskToScheduler(jobSchedularsArray[2]+""+orgnId+""+userName, runnableTask, String.valueOf(jobSchedularsArray[4]));
              
                String aaa =  request.getParameter("enableNotifications");
                
                if ( request.getParameter("enableNotifications")!=null 
                		&& "on".equalsIgnoreCase(request.getParameter("enableNotifications")) ) {
            		String jobName = request.getParameter("jobName");
            		String toEmail = request.getParameter("notificationEmailIds");
            		String subject = "ETL Job Scheduled";
            		java.util.Date scheduleStartDate = (Date)jobSchedularsArray[9];
            		DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");  
            		String scheduleStartDateStr = dateFormat.format(scheduleStartDate);  
            		String body = "Job "+jobName+" has been scheduled to run at "+ scheduleStartDateStr;
            		notificationUtills.sendMailNotification(toEmail, subject, body);
            	}
                
            }
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
    }

    // Remove scheduled task 
    public void removeTaskFromScheduler(Object id) {
        ScheduledFuture<?> scheduledTask = jobsMap.get(id);
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
            //jobsMap.remove(id);
            jobsMap.put(id, null);
        }
    }

    // A context refresh event listener
    @EventListener({ContextRefreshedEvent.class})
    void contextRefreshedEvent() {
        try {
            jobsMap = new HashMap<>();
            List<Object[]> jobSchedularsList = jobSchedulingProcessDAO.getJobSchedulars();
            if (jobSchedularsList != null && !jobSchedularsList.isEmpty()) {
                for (Object[] jobSchedularsArray : jobSchedularsList) {
                    if (jobSchedularsArray != null
                            && jobSchedularsArray.length != 0
                            && jobSchedularsArray[4] != null
                            && !"".equalsIgnoreCase(String.valueOf(jobSchedularsArray[4]))
                            && !"null".equalsIgnoreCase(String.valueOf(jobSchedularsArray[4]))
                            && CronExpression.isValidExpression(String.valueOf(jobSchedularsArray[4]))) {
                        Runnable runnableTask = () -> {
                            try {
                                executeJob(request, jobSchedularsArray);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }                           

                        };
                        addTaskToScheduler(jobSchedularsArray[2]+""+jobSchedularsArray[0]+""+jobSchedularsArray[1], runnableTask,String.valueOf(jobSchedularsArray[4]));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Get all tasks from DB and reschedule them in case of context restarted
    }

    public JSONObject getActiveScheduledJobs(HttpServletRequest request) {
         JSONObject scheduledJobObj = new JSONObject();
        try {
            JSONArray jobScheduledArray = new JSONArray();
            JSONObject tableObj = new JSONObject();
            tableObj.put("id", "RUNNING_JOBS");
            tableObj.put("text", "Running Jobs");
            tableObj.put("value", "RUNNING_JOBS");
            jobScheduledArray.add(tableObj);
            tableObj = new JSONObject();
            tableObj.put("id", "STOPED_JOBS");
            tableObj.put("text", "Stoped Jobs");
            tableObj.put("value", "STOPED_JOBS");
            jobScheduledArray.add(tableObj);
             List<Object[]> jobSchedularsList = jobSchedulingProcessDAO.getActiveScheduledJobs((String)request.getSession(false).getAttribute("ssOrgId"),
                     (String)request.getSession(false).getAttribute("ssUsername"));
            
             if (jobSchedularsList != null && !jobSchedularsList.isEmpty()) {
                 
                 for (Object[] scheduledJobsArray : jobSchedularsList) {
                     if (scheduledJobsArray != null && scheduledJobsArray.length != 0) {
                         tableObj = new JSONObject();
                         tableObj.put("id", scheduledJobsArray[2]);
                         tableObj.put("text", scheduledJobsArray[3]);
                         tableObj.put("value",scheduledJobsArray[2]);
                         if ("Y".equalsIgnoreCase(String.valueOf(scheduledJobsArray[6]))) {
                             tableObj.put("parentid", "RUNNING_JOBS");
                         }else{
                             tableObj.put("parentid", "STOPED_JOBS");
                         }
                         jobScheduledArray.add(tableObj);
                     }
                 }
                
                // tableObj.put("icon", fromConnectObj.get("imageIcon"));//imageIcon
            }
             scheduledJobObj.put("scheduledJobsArray", jobScheduledArray);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scheduledJobObj;
    }

    public JSONObject processScheduledJobFlag(HttpServletRequest request) {
        JSONObject scheduledJobObj = new JSONObject();
        try {
            String message = "";
            String jobId =  request.getParameter("jobId");
            String jobFlag =  request.getParameter("jobFlag");
            if ("STOP".equalsIgnoreCase(jobFlag)) {
                int updateCount = jobSchedulingProcessDAO.updateActiveStatus((String) request.getSession(false).getAttribute("ssOrgId"),
                        request.getParameter("jobId"),
                        (String) request.getSession(false).getAttribute("ssUsername"),
                        "N"
                );
                   if (updateCount != 0) {
                       message = "Selected Job has been stoped successfully.";
                       removeTaskFromScheduler(jobId + ""
                               + "" + request.getSession(false).getAttribute("ssOrgId") + ""
                               + "" + request.getSession(false).getAttribute("ssUsername"));
                   }else{
                       message = "Selected Job failed to stop.";
                   }
               
            }else if ("RUN".equalsIgnoreCase(jobFlag)) {
                int updateCount = jobSchedulingProcessDAO.updateActiveStatus((String) request.getSession(false).getAttribute("ssOrgId"),
                        request.getParameter("jobId"),
                        (String) request.getSession(false).getAttribute("ssUsername"),
                        "Y"
                );
                if (updateCount != 0) {
                    message = "Selected Job is running.";
                    addTaskToScheduler(request, jobId, (String) request.getSession(false).getAttribute("ssOrgId"),
                            (String) request.getSession(false).getAttribute("ssUsername"));
                }else{
                    message = "Selected Job failed to stop.";
                }
               
            }else if ("DELETE".equalsIgnoreCase(jobFlag)) {
                int deleteCount = jobSchedulingProcessDAO.delteJobSchedular((String) request.getSession(false).getAttribute("ssOrgId"),
                        request.getParameter("jobId"),
                        (String) request.getSession(false).getAttribute("ssUsername")
                );
                if (deleteCount != 0) {
                    message = "Selected Job schedular has been removed successfully .";
                    removeTaskFromScheduler(jobId + ""
                            + "" + request.getSession(false).getAttribute("ssOrgId") + ""
                            + "" + request.getSession(false).getAttribute("ssUsername"));
                }else{
                    message = "Failed to remove the job schedular";
                }
            }
            scheduledJobObj.put("message",message);
            scheduledJobObj.put("flag", true);
        } catch (Exception e) {
            scheduledJobObj.put("message", e.getMessage());
            scheduledJobObj.put("flag", false);
            e.printStackTrace();
        }
        return scheduledJobObj;
    }
    
    public Map fetchJobScheduleInfo(HttpServletRequest request) {
        Map scheduledJobObj = new LinkedHashMap();
        try {
        	String jobId =  request.getParameter("jobId");
        	scheduledJobObj = jobSchedulingProcessDAO.fetchJobScheduleInfo(jobId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scheduledJobObj;
    }
    
    public JSONObject validateMergeComponentTrfmRules(HttpServletRequest request) {
       JSONObject resultObject =  new JSONObject();
       Boolean columnsMatched = true;
        try {
        	String flowchartDataString = request.getParameter("flowchartData");
        	JSONObject flowchartData = (JSONObject)JSONValue.parse(flowchartDataString);
        	JSONObject operators = (JSONObject)flowchartData.get("operators");
        	
        	JSONObject sourceOperatorData  = new JSONObject();
        	JSONObject targetOperatorData  = new JSONObject();
        	
        	for (Object operatorId : operators.keySet()) {
        		JSONObject operatorData = (JSONObject)operators.get(operatorId);
        		JSONArray connectedFromOperatorIds = processJobComponentsService.getConnectedFromOperatorIds(request,(String)operatorId, flowchartData);
        		if (connectedFromOperatorIds.size() == 0) {
        			sourceOperatorData = operatorData;
        		} else {
        			String component = String.valueOf( operatorData.get("component") ) ;
        			if (component!=null && "Y".equalsIgnoreCase(component)) {
        				
        			} else {
        				targetOperatorData = operatorData;
        			}
        		}
        	}
        	
        	// getting source columns
        	List sourceColumnsList = new ArrayList<>();
        	String sourceTableName = (String)sourceOperatorData.get("tableName");
        	JSONObject sourceConnObj = (JSONObject)sourceOperatorData.get("connObj");
        	Connection srcConnection =  (Connection)componentUtilities.getConnection(sourceConnObj);
        	
        	 PreparedStatement preparedStatement = srcConnection.prepareStatement("SELECT ^ FROM "+sourceTableName);
        	 ResultSet rs = preparedStatement.executeQuery();
             //Retrieving the ResultSetMetadata object
             ResultSetMetaData rsMetaData = rs.getMetaData();
             System.out.println("List of column names in the current table: ");
             //Retrieving the list of column names
             int count = rsMetaData.getColumnCount();
             for(int i = 1; i<=count; i++) {
                System.out.println(rsMetaData.getColumnName(i));
                sourceColumnsList.add(rsMetaData.getColumnName(i));
             }
        	
          // getting dest columns
             List destColumnsList = new ArrayList<>();
         	String destTableName = (String)targetOperatorData.get("tableName");
         	JSONObject destConnObj = (JSONObject)targetOperatorData.get("connObj");
         	Connection destConnection =  (Connection)componentUtilities.getConnection(destConnObj);
         	
         	 PreparedStatement preparedStatement1 = srcConnection.prepareStatement("SELECT ^ FROM "+destTableName);
         	 ResultSet rs1 = preparedStatement.executeQuery();
              //Retrieving the ResultSetMetadata object
              ResultSetMetaData rsMetaData1 = rs1.getMetaData();
              System.out.println("List of column names in the current table: ");
              //Retrieving the list of column names
              int count1 = rsMetaData1.getColumnCount();
              for(int i = 1; i<=count1; i++) {
                 System.out.println(rsMetaData1.getColumnName(i));
                 destColumnsList.add(rsMetaData1.getColumnName(i));
              }
             
              if (destColumnsList.size() == sourceColumnsList.size()) {
            	  for (int i=0; i< destColumnsList.size();i++) {
              		String destColumn = (String)destColumnsList.get(i);
              		if (!sourceColumnsList.contains(destColumn) )  {
              			columnsMatched = false;
              			break;
              		} 
              		
              		}
              } else {
            	  columnsMatched = false;
              }
        	
        	if (columnsMatched) {
        		resultObject.put("message", "Success");
        	} else {
        		resultObject.put("message", "Mismatch");
        	}
        	
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }
    
     Map<Object, ScheduledFuture<?>> jobsMap = new HashMap<>();
    public JSONObject scheduleProcessJob(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            String jobId = request.getParameter("jobId");
            String jobName = request.getParameter("jobName");
            String cronExp = request.getParameter("cronExp");
            String cronStartDate = request.getParameter("cronStartDate");
            String cronEndDate = request.getParameter("cronEndDate");
            JSONObject paramData = new JSONObject();
            paramData.put("cronStartDate", cronStartDate);
            paramData.put("cronEndDate", cronEndDate);
            String orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            String userName = (String) request.getSession(false).getAttribute("ssUsername");
            resultObj = jobSchedulingProcessDAO.insertJobs(orgnId, userName, jobId, jobName, cronExp,paramData);
            if (resultObj != null && !resultObj.isEmpty() && (boolean)resultObj.get("flag")) {
                addTaskToScheduler(request, jobId, orgnId, userName);
            }
        } catch (org.hibernate.exception.ConstraintViolationException ex) {
            resultObj.put("flag", false);
            resultObj.put("message", "The Selected Job is in-process,please go the Schedule Jobs tabs and check the progress.");
        } catch (Exception e) {
            resultObj.put("flag", false);
            resultObj.put("message", e.getMessage());
            e.printStackTrace();
        }
        return resultObj;
    }
    
    
    
}
