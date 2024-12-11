/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.google.gson.JsonArray;
import com.pilog.mdm.DAO.IVisionTransformProcessJobComponentsDAO;
import com.pilog.mdm.DAO.V10JobSchedulingProcessDAO;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.transformcomputilities.NotificationUtills;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;
import com.sap.mw.jco.JCO;
import org.apache.commons.collections.map.HashedMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


/**
 * @author Ravindar.P
 */
@Service
public class IVisionTransformProcessJobComponentsService {

	@Value("${jdbc.driver}")
	private String dataBaseDriver;
	@Value("${jdbc.username}")
	private String userName;
	@Value("${jdbc.password}")
	private String password;
	@Value("${jdbc.url}")
	private String dbURL;

	// @Value("${etl.file.path}")
	private String etlFilePath;

	{
		if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
			etlFilePath = "C://";
		} else {
			etlFilePath = "/u01/";
		}
	}

	// private Map<String, Thread> asyncJobMap = new HashMap<>();
	private Map<String, ExecutorService> asyncJobMap = new HashMap<>();
	@Autowired
	IVisionTransformProcessJobComponentsDAO processJobComponentsDAO;

	@Autowired
	IVisionTransformComponentUtilities componentUtilities;

	@Autowired
	IVisionTransformERPProcessJobService erpProcessJobService;

	@Autowired
	IVisionTransformFileProcessJobService fileProcessJobService;

	@Autowired
	V10JobSchedulingProcessDAO jobSchedulingProcessDAO;

	@Autowired
	private DataAccess access;

	@Autowired
	private NotificationUtills notificationUtills;

	public JSONObject processJobComponents(HttpServletRequest request, JSONObject flowchartData, String jobId) {

		final int numberOfThreads = 100;
		final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		final List<Future<?>> futures = new ArrayList<>();

		java.util.Date jobStartTime = new java.util.Date();
		long jobStartTimeLong = System.currentTimeMillis();
		Connection currConnection = null;
		JSONObject resultObj = new JSONObject();
		List dropTablesList = new ArrayList();

//        asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--" + request.getSession(false).getAttribute("ssOrgId"), Thread.currentThread());
		String orgId = (String) request.getSession(false).getAttribute("ssOrgId");
		String userName = (String) request.getSession(false).getAttribute("ssUsername");
		asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--"
				+ request.getSession(false).getAttribute("ssOrgId"), executor);
		System.out.println(
				"Main Thread :: " + Thread.currentThread().getName() + " :: " + Thread.currentThread().getName());
		request.setAttribute("stagingTablesList", new ArrayList());
		if ("Y".equalsIgnoreCase(String.valueOf(flowchartData.get("scheduleJob")))) {
			if (flowchartData.get("enableNotifications") != null
					&& "on".equalsIgnoreCase(String.valueOf(flowchartData.get("enableNotifications")))) {
				String toEmail = String.valueOf(flowchartData.get("notificationEmailIds"));
				String jobName = request.getParameter("jobName");
				String subject = "ETL Scheduled Job Started";
				String body = "Scheduled Job " + jobName + " has stated";
				notificationUtills.sendMailNotification(toEmail, subject, body);

			}
		}
		try {
			try {
				componentUtilities.deleteProcesslog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
			}
			try {
				componentUtilities.deleteProcessBarlog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
			}
			try {
				componentUtilities.deleteJobProcessSteps((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
			}

			String preview = request.getParameter("preview");
			if (preview != null && "Y".equalsIgnoreCase(preview)) {
				try {
					componentUtilities.deleteJobPreview((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
				} catch (Exception e) {
				}
			}

			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "Started Job Execution", "INFO", 20,
						"Y", jobId);
			} catch (Exception ex) {
			}

			request.setAttribute("jobStartTime", System.currentTimeMillis());
//            String flowchartDataStr = request.getParameter("flowchartData");
//            JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
			JSONObject operatorsObj = (JSONObject) flowchartData.get("operators");
			Map operators1 = new HashedMap();
			operators1.putAll(operatorsObj);
			Map operators = componentUtilities.sortOperators(operators1);
			Map skipOperatorsMap = new HashMap();

			String nextExecutionSeq = "";
			List<String> keyList = new ArrayList(operators.keySet());
			int index = 0;
			for (Object operatorId : keyList) {

				JSONObject operator = (JSONObject) operators.get(operatorId);
				String currentExecutionSeq = String.valueOf(operator.get("executionSequence"));
				int operatorIndex = keyList.indexOf(operatorId);
				if (operatorIndex != (keyList.size() - 1)) {
					String nextOperatorId = keyList.get(operatorIndex + 1);
					JSONObject nextOperator = (JSONObject) operators.get(nextOperatorId);
					nextExecutionSeq = String.valueOf(nextOperator.get("executionSequence"));
				} else {
					nextExecutionSeq = "";
				}

				if (nextExecutionSeq != null && !"".equalsIgnoreCase(nextExecutionSeq)
						&& nextExecutionSeq.equalsIgnoreCase(currentExecutionSeq)) {
					futures.add(executor.submit(() -> {
						String threadName = Thread.currentThread().getName();
						System.out.println("threadName  - " + Thread.currentThread().getId());
//                        asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--" + request.getSession(false).getAttribute("ssOrgId") + AuditIdGenerator.genRandom32Hex(), Thread.currentThread());
						System.out.println("job started : Sequence  -" + operator.get("executionSequence") + " :: "
								+ System.currentTimeMillis());
						if (operator != null && operator.size() > 0) {
							String isComponent = (String) operator.get("component");
							String sourceOrTarget = (String) operator.get("sourceOrTarget");
							String iconType = (String) operator.get("iconType");
							JSONArray connectedFromOpArray = new JSONArray();

							/*
							 * try { componentUtilities.processBarETLLog((String)
							 * request.getSession(false).getAttribute("ssUsername"), (String)
							 * request.getSession(false).getAttribute("ssOrgId"), "STEPSTART", 20, "Y",
							 * jobId, "N", "20", String.valueOf(operator.get("operatorId"))); } catch
							 * (Exception ex) { ex.printStackTrace(); }
							 */
							try {
								componentUtilities.processETLLogSteps(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										String.valueOf(operatorId), "START", "", "Y", jobId);
							} catch (Exception ex) {
							}

//                        connectedFromOpArray = (JSONArray) operator.get("connectedFrom");
							connectedFromOpArray = getConnectedFromOperatorIds(request,
									String.valueOf(operator.get("operatorId")), flowchartData);

							if (isComponent != null && "Y".equalsIgnoreCase(isComponent)) {

								if (iconType != null && "UNIQUE".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processUniqueComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "GROUP_JOB".equalsIgnoreCase(iconType)) {

									if (operator != null && operator.size() > 0) {
										String subJobId = String.valueOf(operator.get("jobId"));
										String subJobDesc = String.valueOf(operator.get("jobDesc"));
										JSONObject jobFlowChartDataObj = processJobComponentsDAO
												.getJobTransformationRules(request, subJobId);
										String jobFlowChartDataStr = (String) jobFlowChartDataObj
												.get("mappedObjectData");

										JSONObject jobFlowChartData = (JSONObject) JSONValue.parse(jobFlowChartDataStr);

										try {

											componentUtilities.processETLLog(
													(String) request.getSession(false).getAttribute("ssUsername"),
													(String) request.getSession(false).getAttribute("ssOrgId"),
													subJobDesc + " Job Execution Started", "INFO", 20, "Y", jobId);
										} catch (Exception ex) {
											ex.printStackTrace();
										}

										JSONObject jobResultObj = processJobComponents(request, jobFlowChartData,
												subJobId);

										String resultFlag = String.valueOf(jobResultObj.get("resultFlag"));
										if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
												&& resultFlag.equalsIgnoreCase("Success")) {
											try {

												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
											} catch (Exception ex) {
												ex.printStackTrace();
											}

										} else if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
												&& resultFlag.equalsIgnoreCase("Fail")) {
											try {

												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
											} catch (Exception ex) {
												ex.printStackTrace();
											}

										}

									}

								} else if (iconType != null && "API".equalsIgnoreCase(iconType)) {
//									String stagingTable = (String) operator.get("tableName");
//									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
//									request.setAttribute("stagingTablesList",
//											request.getAttribute("stagingTablesList"));
									processAPIComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "GROUP".equalsIgnoreCase(iconType)) { // DENORMALIZE
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processDenormalizeComponentJob(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "UNGROUP".equalsIgnoreCase(iconType)) { // NORMALIZE
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processNormalizeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "FILTER".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processFilterComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "OUTPUT".equalsIgnoreCase(iconType)) {
									processOutputComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "STAGING".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processStagingComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "MERGE_FILES".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processMergeFilesComponentJob(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "JOIN".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJoinsComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SORT".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processSortComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "GROUPBY".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processGroupByComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "QUERY".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processQueryComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SCD".equalsIgnoreCase(iconType)) {

									JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
									String selectedSCDType = (String) trfmRules.get("selectedSCDType");
									if (selectedSCDType != null && "SCD1".equalsIgnoreCase(selectedSCDType)) {

//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);

										processSCD1ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD2".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);

										processSCD2ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD3".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processSCD3ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);

									} else if (selectedSCDType != null && "SCD4".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processSCD4ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD6".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processSCD6ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									}
								} else if (iconType != null && "MERGE".equalsIgnoreCase(iconType)) {
//                            JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);

									JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
									String operatorType = (String) trfmRules.get("trfmRules");
									if (operatorType != null && "Insert".equalsIgnoreCase(operatorType)) {
//                                JSONArray connectedToArray = (JSONArray) operator.get("connectedTo");
										JSONArray connectedToArray = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										List rejectedOperatorsList = new ArrayList();
										boolean hasRejectedComponent = operators.values().stream().anyMatch(op -> {
											if ("REJECTED".equalsIgnoreCase(
													String.valueOf(((JSONObject) op).get("iconType")))) {
//                                        JSONArray connectedFromArray = (JSONArray) ((JSONObject) op).get("connectedFrom");
												JSONArray connectedFromArray = (JSONArray) getConnectedFromOperatorIds(
														request, String.valueOf(((JSONObject) op).get("operatorId")),
														flowchartData);
												String connectedFrom = (String) connectedFromArray.get(0);
												if (connectedFrom.equalsIgnoreCase(String.valueOf(operatorId))) {
													rejectedOperatorsList.add(op);
													return true;
												} else {
													return false;
												}
											} else {
												return false;
											}

										});
										if (hasRejectedComponent) {
											JSONObject rejectedOperator = (JSONObject) rejectedOperatorsList.get(0);
											String rejectedOperatorId = String
													.valueOf(rejectedOperator.get("operatorId"));
											JSONArray skipOperators = (JSONArray) request.getAttribute(
													"skipOperators_" + jobId + "_" + operator.get("operatorId"));
											if (skipOperators != null) {
												skipOperators.add(rejectedOperatorId);
											}
//                                            request.setAttribute("skipOperators", skipOperators);
											request.setAttribute(
													"skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
													skipOperatorIds);
											processRejectedComponentJob(request, (String) rejectedOperatorId, operators,
													flowchartData, jobId);

										}
									}

									processMergeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "REJECTED".equalsIgnoreCase(iconType)) {
									processRejectedComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SAPLOAD".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"), skipOperatorIds);
//                                    JSONArray skipOperators11 = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId+ "_" + operator.get("operatorId"));

									processOneToOneComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SAPLOADREVERSE".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"), skipOperatorIds);
//                                    JSONArray skipOperators11 = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId+ "_" + operator.get("operatorId"));

									processOneToOneComponentJobReverse(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "SAPLOADSTANDARD".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"), skipOperatorIds);
//                                    JSONArray skipOperators11 = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId+ "_" + operator.get("operatorId"));

									processOneToOneComponentJobStandard(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "SAPRESUME".equalsIgnoreCase(iconType)) {

									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
									processSapResumeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SAPJOIN".equalsIgnoreCase(iconType)) {

									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
									processSapJoinComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "PIVOT".equalsIgnoreCase(iconType)) {
//                                    JSONArray skipOperatorIds = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")), flowchartData);
//                                    request.setAttribute("skipOperators", skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
									processPivotComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "UNPIVOT".equalsIgnoreCase(iconType)) {
//                                    JSONArray skipOperatorIds = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")), flowchartData);
//                                    request.setAttribute("skipOperators", skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
									processUnpivotComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "ROWS_RANGE".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processRowsRangeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "VALIDATE".equalsIgnoreCase(iconType)) {
//									String stagingTable = (String) operator.get("tableName");
//									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
//									request.setAttribute("stagingTablesList",
//											request.getAttribute("stagingTablesList"));
									processValidateComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "FETCH".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processfetchComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "FILE_FOLDER".equalsIgnoreCase(iconType)) {
									processFileFolderComponent(request, (String) operatorId, operators, flowchartData,
											jobId);
								}
								

							} else if (!connectedFromOpArray.isEmpty()) {
//                                JSONArray skipOperators = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"));
								JSONArray skipOperators = (JSONArray) request
										.getAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"));
//                        long operatorIdLong = Long.valueOf(String.valueOf(operatorId));
								if (!(skipOperators != null && skipOperators.contains(operatorId))) {
									request.removeAttribute("skipOperators_" + jobId);
									processNonComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);
								}
							} else if (connectedFromOpArray.isEmpty()) {
								insertReconciliation(request, (String) operatorId, operators, flowchartData, jobId);
							}
						}

						try {
							String threadNewName = Thread.currentThread().getName();
							String stepStatus = threadNewName.endsWith("_ERROR") ? "FAIL" : "SUCCESS";
							componentUtilities.processETLLogSteps(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									String.valueOf(operatorId), "END", stepStatus, "Y", jobId);
						} catch (Exception ex) {
						}

					}));
				} else {
					futures.add(executor.submit(() -> {
						String threadName = Thread.currentThread().getName();
						System.out.println("threadName  - " + Thread.currentThread().getId());
//                        asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--" + request.getSession(false).getAttribute("ssOrgId") + AuditIdGenerator.genRandom32Hex(), Thread.currentThread());
						System.out.println("job started : Sequence  -" + operator.get("executionSequence") + " :: "
								+ System.currentTimeMillis());
						if (operator != null && operator.size() > 0) {
							String isComponent = (String) operator.get("component");
							String sourceOrTarget = (String) operator.get("sourceOrTarget");
							String iconType = (String) operator.get("iconType");
							JSONArray connectedFromOpArray = new JSONArray();

							/*
							 * try { componentUtilities.processBarETLLog((String)
							 * request.getSession(false).getAttribute("ssUsername"), (String)
							 * request.getSession(false).getAttribute("ssOrgId"), "STEPSTART", 20, "Y",
							 * jobId, "N", "20", String.valueOf(operator.get("operatorId"))); } catch
							 * (Exception ex) { ex.printStackTrace(); }
							 */
							try {
								componentUtilities.processETLLogSteps(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										String.valueOf(operatorId), "START", "", "Y", jobId);
							} catch (Exception ex) {
							}

//                        connectedFromOpArray = (JSONArray) operator.get("connectedFrom");
							connectedFromOpArray = getConnectedFromOperatorIds(request,
									String.valueOf(operator.get("operatorId")), flowchartData);

							if (isComponent != null && "Y".equalsIgnoreCase(isComponent)) {

								if (iconType != null && "UNIQUE".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processUniqueComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "GROUP_JOB".equalsIgnoreCase(iconType)) {

									if (operator != null && operator.size() > 0) {
										String subJobId = String.valueOf(operator.get("jobId"));
										String subJobDesc = String.valueOf(operator.get("jobDesc"));
										JSONObject jobFlowChartDataObj = processJobComponentsDAO
												.getJobTransformationRules(request, subJobId);
										String jobFlowChartDataStr = (String) jobFlowChartDataObj
												.get("mappedObjectData");

										JSONObject jobFlowChartData = (JSONObject) JSONValue.parse(jobFlowChartDataStr);

										try {

											componentUtilities.processETLLog(
													(String) request.getSession(false).getAttribute("ssUsername"),
													(String) request.getSession(false).getAttribute("ssOrgId"),
													subJobDesc + " Job Execution Started", "INFO", 20, "Y", jobId);
										} catch (Exception ex) {
											ex.printStackTrace();
										}

										JSONObject jobResultObj = processJobComponents(request, jobFlowChartData,
												subJobId);

										String resultFlag = String.valueOf(jobResultObj.get("resultFlag"));
										if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
												&& resultFlag.equalsIgnoreCase("Success")) {
											try {

												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
											} catch (Exception ex) {
												ex.printStackTrace();
											}

										} else if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
												&& resultFlag.equalsIgnoreCase("Fail")) {
											try {

												componentUtilities.processETLLog(
														(String) request.getSession(false).getAttribute("ssUsername"),
														(String) request.getSession(false).getAttribute("ssOrgId"),
														subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
											} catch (Exception ex) {
												ex.printStackTrace();
											}

										}

									}

								} else if (iconType != null && "API".equalsIgnoreCase(iconType)) {
//									String stagingTable = (String) operator.get("tableName");
//									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
//									request.setAttribute("stagingTablesList",
//											request.getAttribute("stagingTablesList"));
									processAPIComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "GROUP".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processDenormalizeComponentJob(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "UNGROUP".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processNormalizeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "FILTER".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processFilterComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "OUTPUT".equalsIgnoreCase(iconType)) {
									processOutputComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "STAGING".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processStagingComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "MERGE_FILES".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processMergeFilesComponentJob(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "JOIN".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJoinsComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SORT".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processSortComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "GROUPBY".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processGroupByComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "QUERY".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processQueryComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SCD".equalsIgnoreCase(iconType)) {

									JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
									String selectedSCDType = (String) trfmRules.get("selectedSCDType");
									if (selectedSCDType != null && "SCD1".equalsIgnoreCase(selectedSCDType)) {

//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
//                                        request.setAttribute("skipOperators", skipOperatorIds);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processSCD1ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD2".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);

										processSCD2ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD3".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processSCD3ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);

									} else if (selectedSCDType != null && "SCD4".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processSCD4ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD6".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processSCD6ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									}
								} else if (iconType != null && "MERGE".equalsIgnoreCase(iconType)) {
//                            JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);

									JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
									String operatorType = (String) trfmRules.get("operatorType");
									if (operatorType != null && "Insert".equalsIgnoreCase(operatorType)) {
//                                JSONArray connectedToArray = (JSONArray) operator.get("connectedTo");
										JSONArray connectedToArray = getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										List rejectedOperatorsList = new ArrayList();
										boolean hasRejectedComponent = operators.values().stream().anyMatch(op -> {
											if ("REJECTED".equalsIgnoreCase(
													String.valueOf(((JSONObject) op).get("iconType")))) {
//                                        JSONArray connectedFromArray = (JSONArray) ((JSONObject) op).get("connectedFrom");
												JSONArray connectedFromArray = (JSONArray) getConnectedFromOperatorIds(
														request, String.valueOf(((JSONObject) op).get("operatorId")),
														flowchartData);
												String connectedFrom = (String) connectedFromArray.get(0);
												if (connectedFrom.equalsIgnoreCase(String.valueOf(operatorId))) {
													rejectedOperatorsList.add(op);
													return true;
												} else {
													return false;
												}
											} else {
												return false;
											}

										});
										if (hasRejectedComponent) {
											JSONObject rejectedOperator = (JSONObject) rejectedOperatorsList.get(0);
											String rejectedOperatorId = String
													.valueOf(rejectedOperator.get("operatorId"));
											JSONArray skipOperators = (JSONArray) request.getAttribute(
													"skipOperators_" + jobId + "_" + operator.get("operatorId"));
											if (skipOperators != null) {
												skipOperators.add(rejectedOperatorId);
											}
											request.setAttribute(
													"skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
													skipOperatorIds);
											processRejectedComponentJob(request, (String) rejectedOperatorId, operators,
													flowchartData, jobId);

										}
									}

									processMergeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "REJECTED".equalsIgnoreCase(iconType)) {

									processRejectedComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SAPLOAD".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
									processOneToOneComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SAPLOADREVERSE".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"), skipOperatorIds);
//                                    JSONArray skipOperators11 = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId+ "_" + operator.get("operatorId"));

									processOneToOneComponentJobReverse(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "SAPLOADSTANDARD".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"), skipOperatorIds);
//                                    JSONArray skipOperators11 = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId+ "_" + operator.get("operatorId"));

									processOneToOneComponentJobStandard(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "SAPRESUME".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
//                                    request.setAttribute("skipOperators", skipOperatorIds);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
									processSapResumeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SAPJOIN".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
//                                    request.setAttribute("skipOperators", skipOperatorIds);
									request.setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
											skipOperatorIds);
									processSapJoinComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "PIVOT".equalsIgnoreCase(iconType)) {
//                                    JSONArray skipOperatorIds = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")), flowchartData);
//                                    request.setAttribute("skipOperators", skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
									processPivotComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "UNPIVOT".equalsIgnoreCase(iconType)) {
//                                    JSONArray skipOperatorIds = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")), flowchartData);
//                                    request.setAttribute("skipOperators", skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
									processUnpivotComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "ROWS_RANGE".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processRowsRangeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);
								} else if (iconType != null && "VALIDATE".equalsIgnoreCase(iconType)) {
//									String stagingTable = (String) operator.get("tableName");
//									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
//									request.setAttribute("stagingTablesList",
//											request.getAttribute("stagingTablesList"));
									processValidateComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "FETCH".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processfetchComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "FILE_FOLDER".equalsIgnoreCase(iconType)) {
									processFileFolderComponent(request, (String) operatorId, operators, flowchartData,
											jobId);
								}
							} else if (!connectedFromOpArray.isEmpty()) { // target
								JSONArray skipOperators = (JSONArray) request
										.getAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"));
								if (!(skipOperators != null && skipOperators.contains(operatorId))) {
//                                    request.removeAttribute("skipOperators");
									request.removeAttribute("skipOperators_" + jobId);
									processNonComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);
								}
							} else if (connectedFromOpArray.isEmpty()) { // source operator
								insertReconciliation(request, (String) operatorId, operators, flowchartData, jobId);
							}

						}

						try {
							String threadNewName = Thread.currentThread().getName();
							String stepStatus = threadNewName.endsWith("_ERROR") ? "FAIL" : "SUCCESS";
							componentUtilities.processETLLogSteps(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									String.valueOf(operatorId), "END", stepStatus, "Y", jobId);
						} catch (Exception ex) {
						}

					}));

					if (futures != null && !futures.isEmpty()) {
						try {
							for (Future<?> future : futures) {
								future.get();
								// do anything you need, e.g. isDone(), ...
							}
							futures.clear();
						} catch (InterruptedException | ExecutionException e) {
							e.printStackTrace();
						}
					}
				}

				/*
				 * try { componentUtilities.processBarETLLog((String)
				 * request.getSession(false).getAttribute("ssUsername"), (String)
				 * request.getSession(false).getAttribute("ssOrgId"), "STEPCOMPLETE", 20, "Y",
				 * jobId, "Y", "20", String.valueOf(operator.get("operatorId"))); } catch
				 * (Exception ex) { ex.printStackTrace(); }
				 */
				index++;
				if (index == 1) {

				}
			}

			try {

				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "Job Execution Complete", "INFO",
						20, "N", jobId);
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			try {
				componentUtilities.processBarETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "PROCESSCOMPLETE", 20, "N", jobId,
						"Y", "20", "");
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			try {

				componentUtilities.processETLLogSteps((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "", "END", "", "N", jobId);
			} catch (Exception ex) {
			}

			if ("Y".equalsIgnoreCase(String.valueOf(flowchartData.get("scheduleJob")))) {
				long jobEndTimeLong = System.currentTimeMillis();
				long duration = (jobEndTimeLong - jobStartTimeLong) / 1000;
				try {
					jobSchedulingProcessDAO.updateRunningStatus(jobId, "STOP");
				} catch (Exception e) {
				}

				try {

					java.sql.Date sqlDate = new java.sql.Date(jobStartTime.getTime());
					jobSchedulingProcessDAO.updateLastRunningDate(jobId, sqlDate);
				} catch (Exception e) {
				}

				try {

					jobSchedulingProcessDAO.updateLastRunDuration(jobId, duration);
				} catch (Exception e) {
				}

				if (flowchartData.get("enableNotifications") != null
						&& "on".equalsIgnoreCase(String.valueOf(flowchartData.get("enableNotifications")))) {

					String toEmail = String.valueOf(flowchartData.get("notificationEmailIds"));
					String subject = "";
					String body = "";
					String jobName = request.getParameter("jobName");
					if (request.getAttribute(jobId) != null
							&& "Fail".equalsIgnoreCase((String) request.getAttribute(jobId))) {
						subject = "ETL Scheduled Job Failed";
						body = "Scheduled Job " + jobName + " has Failed";
					} else {
						subject = "ETL Scheduled Job Completed";
						body = "Scheduled Job " + jobName + " has Completed";
					}

					notificationUtills.sendMailNotification(toEmail, subject, body);

				}

			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), e.getMessage(), "Error", 20, "Y",
						request.getParameter("jobId"));

			} catch (Exception ex) {
			}

			try {
				componentUtilities.processETLLogSteps((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "", "END", "", "N", jobId);
			} catch (Exception ex) {
			}
			request.setAttribute(jobId, "Fail");
		} finally {

			currConnection = componentUtilities.getCurrentConnection();
			List stagingTablesList = (List) request.getAttribute("stagingTablesList");
			for (int i = 0; i < stagingTablesList.size(); i++) {
				String dropTableName = (String) stagingTablesList.get(i);
				componentUtilities.dropStagingTable(dropTableName, currConnection);
			}
			try {
				if (asyncJobMap != null && !asyncJobMap.isEmpty()) {
//                    asyncJobMap.remove(jobId + (String) request.getSession(false).getAttribute("ssUsername") + request.getSession(false).getAttribute("ssOrgId"));
					String processId = jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--"
							+ request.getSession(false).getAttribute("ssOrgId");
					List<String> keySet = new ArrayList(asyncJobMap.keySet());
					for (String key : keySet) {
						if (key.contains(processId)) {
							asyncJobMap.remove(key);
						}
					}
				}
			} catch (Exception e) {
			}
			try {
				if (currConnection != null) {
					currConnection.close();
				}
			} catch (Exception ex) {
			}
			if (request.getAttribute(jobId) != null && "Fail".equalsIgnoreCase((String) request.getAttribute(jobId))) {
				resultObj.put("resultFlag", "Fail");
			} else {
				resultObj.put("resultFlag", "Success");
			}

		}
		return resultObj;
	}

	public void insertReconciliation(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {
		try {
//			final int numberOfThreads = 1;
//			final ExecutorService executor = Executors.newFixedThreadPool(1);
//			final List<Future<?>> futures = new ArrayList<>();

			// futures.add(executor.submit(() -> {
			JSONObject operator = (JSONObject) operators.get(operatorId);
			String subJobId = String.valueOf(operator.get("subJobId"));
			String preview = request.getParameter("preview");

			componentUtilities.processETLReconciliationDelete(
					(String) request.getSession(false).getAttribute("ssUsername"),
					(String) request.getSession(false).getAttribute("ssOrgId"), jobId, subJobId);

			if ("File".equalsIgnoreCase(String.valueOf(operator.get("dragType")))) {
				int sourceCount = 0;
				int sourceColumnCount = 0;
				JSONObject fileConnObj = (JSONObject) operator.get("connObj");
				String fileName = (String) fileConnObj.get("fileName");
				String filePath = (String) fileConnObj.get("filePath");
				String fileType = (String) fileConnObj.get("fileType");

				if (fileType != null && !"null".equalsIgnoreCase(fileType)
						&& ("XLSX".equalsIgnoreCase(fileType) || "XLS".equalsIgnoreCase(fileType))) {
					sourceCount = fileProcessJobService.excelFileRowCount(request, filePath, fileName);
				} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && ("CSV".equalsIgnoreCase(fileType)
						|| "TXT".equalsIgnoreCase(fileType) || "JSON".equalsIgnoreCase(fileType))) {
					sourceCount = fileProcessJobService.csvJsonTxtFileRowCount(request, filePath, fileName, fileType);
				} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "XML".equalsIgnoreCase(fileType)) {
					sourceCount = fileProcessJobService.xmlFileRowCount(request, filePath, fileName);
				}
				List fromColumnsList = componentUtilities.getHeadersOfImportedFile(request, filePath);
				String originalTableName = (String) operator.get("originalTableName");
				sourceColumnCount = fromColumnsList.size();
				componentUtilities.processETLReconciliation(
						(String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId, originalTableName,
						String.valueOf(sourceCount), null, null, null, null, subJobId,
						String.valueOf(sourceColumnCount), null);
				if (preview != null && "Y".equalsIgnoreCase(preview)) {

					componentUtilities.processETLJobPreview(
							(String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId,
							String.valueOf(operator.get("operatorId")), String.valueOf(sourceCount));

				}

			} else {

				JSONObject connectionObj = (JSONObject) operator.get("connObj");
				String connectionType = (String) connectionObj.get("CONN_CUST_COL1");
				int sourceCount = 0;
				int sourceColumnCount = 0;
				String tableName = (String) operator.get("tableName");
				if (connectionType != null && ("SAP_ECC".equalsIgnoreCase(connectionType)
						|| "SAP_HANA".equalsIgnoreCase(connectionType))) {

					JCO.Client sapConnection = (JCO.Client) componentUtilities.getConnection(connectionObj);
					List columnsList = erpProcessJobService.getSAPTableColumnsWithType(request, sapConnection,
							tableName);
					if (columnsList != null && !columnsList.isEmpty()) {
						Object[] columnsArray = (Object[]) columnsList.get(0);
						String columnName = (String) columnsArray[2];
						List fromColumnsList = new ArrayList();
						fromColumnsList.add(columnName);
						sourceCount = erpProcessJobService.getSapTableRowCount(request, sapConnection, fromColumnsList,
								tableName, null);
					}

				} else {

					if (tableName.contains(".")) {
						// tableName = tableName.split("\\.")[1];
					}
					String query = "SELECT COUNT(*) FROM " + tableName;
					List dataList = processJobComponentsDAO.getTableDataWithQuery(request, query, connectionObj, null);
					BigDecimal count = (BigDecimal) dataList.get(0);
					sourceCount = count.intValue();

					String trimmedTableName = tableName.contains(".") ? tableName.split("\\.")[1] : tableName;
					String owner = tableName.contains(".") ? tableName.split("\\.")[0] : "";

					String columnCountQuery = "SELECT COUNT(*) FROM ALL_TAB_COLUMNS WHERE ";
					if (!"".equalsIgnoreCase(owner)) {
						columnCountQuery += "OWNER='DR1024231' AND ";
					}
					columnCountQuery += " TABLE_NAME = '" + trimmedTableName + "'";
					List columnCountList = processJobComponentsDAO.getTableDataWithQuery(request, columnCountQuery,
							connectionObj, null);
					BigDecimal columnCount = (BigDecimal) columnCountList.get(0);
					sourceColumnCount = columnCount.intValue();

				}

				componentUtilities.processETLReconciliation(
						(String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId, tableName,
						String.valueOf(sourceCount), null, null, null, null, subJobId,
						String.valueOf(sourceColumnCount), null);

				if (preview != null && "Y".equalsIgnoreCase(preview)) {
					componentUtilities.processETLJobPreview(
							(String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), jobId,
							String.valueOf(operator.get("operatorId")), String.valueOf(sourceCount));
				}
			}

			// }));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void updateReconciliation(HttpServletRequest request, String targetTable, String targetCount, String jobId) {
		try {
			final int numberOfThreads = 1;
			final ExecutorService executor = Executors.newFixedThreadPool(1);
			final List<Future<?>> futures = new ArrayList<>();

			futures.add(executor.submit(() -> {
				String updateQuery = "UPDATE ";
			}));

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public JSONObject sapToLocalTable(HttpServletRequest request, JSONObject fromOperator, String jobId) {
		JCO.Client sapConnection = null;
		Connection currentConnection = null;
		PreparedStatement preparedStatement = null;
		try {

			JSONObject sapConnObj = (JSONObject) fromOperator.get("connObj");
			String tableName = (String) fromOperator.get("tableName");
			sapConnection = (JCO.Client) componentUtilities.getConnection(sapConnObj);
			List columnsObj = erpProcessJobService.getSAPTableColumnsWithType(request, sapConnection, tableName);
			List fromColumnsList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[2])
					.collect(Collectors.toList());
			List<Object[]> targetDataTypes = componentUtilities.getTargetDataType("SAP_ECC", "ORACLE");
			JSONObject dataTypeWithLenConvObject = new JSONObject();
			JSONObject dataTypeConvObject = new JSONObject();
			targetDataTypes.stream().forEach(rowData -> dataTypeWithLenConvObject.put(rowData[6],
					rowData[1] + (rowData[2] != null ? ("(" + rowData[2] + ")") : "")));
			targetDataTypes.stream().forEach(rowData -> dataTypeConvObject.put(rowData[6], rowData[1]));

			String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column)
					.collect(Collectors.joining(","));
			Map tableColsObj = new LinkedHashMap();
			tableColsObj.put(tableName, fromColumnsListStr);

			String stagingTableName = "ZZ_TEMP_" + AuditIdGenerator.genRandom32Hex();

			String createTableQuery = "CREATE TABLE " + stagingTableName + "( ";
			createTableQuery += columnsObj.stream().map(rowData -> {
				String columnName = String.valueOf(((Object[]) rowData)[2]);
				if (columnName.contains("/")) {
					columnName = "\"" + columnName + "\"";
				}
				columnName = columnName + " " + dataTypeWithLenConvObject.get(((Object[]) rowData)[3]);
				return columnName;
			}).collect(Collectors.joining(","));
			createTableQuery += " )";
			currentConnection = componentUtilities.getCurrentConnection();
			preparedStatement = currentConnection.prepareStatement(createTableQuery);
			preparedStatement.execute();

			List toColsList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[2])
					.collect(Collectors.toList());
			List toDataTypesList = (List) columnsObj.stream()
					.map(rowData -> dataTypeConvObject.get(((Object[]) rowData)[3])).collect(Collectors.toList());

			((List) request.getAttribute("stagingTablesList")).add(stagingTableName);
			request.setAttribute("stagingTablesList", request.getAttribute("stagingTablesList"));

//            int totalCount = erpProcessJobService.getSapTableRowCount(request, sapConnection, stagingTableName, fromColumnsList, tableColsObj, jobId);
			int start = 0;
			int limit = 0;
			int end = 0;
			JSONObject rowsRangeObj = (JSONObject) fromOperator.get("rowsRangeObj");
			if (rowsRangeObj != null && !rowsRangeObj.isEmpty()) {
				if (String.valueOf(rowsRangeObj.get("start")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("start")))
						&& String.valueOf(rowsRangeObj.get("end")) != null
						&& !"".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))
						&& !"null".equalsIgnoreCase(String.valueOf(rowsRangeObj.get("end")))) {
					try {
						start = Integer.valueOf(String.valueOf(rowsRangeObj.get("start"))) - 1;
						end = Integer.valueOf(String.valueOf(rowsRangeObj.get("end")));
						limit = end - start;
					} catch (Exception ex) {
						start = 0;
						limit = 0;
					}
				}
			}

			String whereClauseCondtion = "";
			JSONArray whereClauseObjArray = (JSONArray) fromOperator.get("whereClauseObjArray");
			if (whereClauseObjArray != null && !whereClauseObjArray.isEmpty()) {
				whereClauseCondtion = (String) whereClauseObjArray.stream().map(whereClauseObject -> {
					String column = (String) ((JSONObject) whereClauseObject).get("column");
					String operator = (String) ((JSONObject) whereClauseObject).get("operator");

					String value = (String) ((JSONObject) whereClauseObject).get("value");
					if (operator != null && "=".equalsIgnoreCase(operator)) {
						value = "'" + value + "'";
					} else if (operator != null && "LIKE".equalsIgnoreCase(operator)) {
						value = "'%" + value + "%'";
					}
					return column + " " + operator + " " + value + "";
				}).collect(Collectors.joining(" AND "));

			}
//            System.out.println("Total records :: " + totalCount);
			JSONObject currentConnectionObj = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName,
					password, "Current_V10");
			List fromDataList = erpProcessJobService.insertSAPDataToDBTable(request, tableName, toColsList,
					sapConnection, currentConnectionObj, preparedStatement, stagingTableName, toDataTypesList, 0,
					whereClauseObjArray, start, limit, end, jobId);

//            int insertcount = sapToLocalTableLimit(request, sapConnection, stagingTableName, preparedStatement, fromColumnsList, toColsList,
//                    toDataTypesList, tableColsObj, 0, 10000, 0, jobId);
			fromOperator.put("originalTableName", fromOperator.get("tableName"));
			fromOperator.put("tableName", stagingTableName);
			fromOperator.put("statusLabel", stagingTableName);
			fromOperator.put("CONN_CUST_COL1", "DB");
			fromOperator.put("connObj",
					new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10"));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (sapConnection != null) {
					JCO.releaseClient(sapConnection);
				}
				if (currentConnection != null) {
					currentConnection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fromOperator;
	}

	public JSONObject otherDBToLocalTable(HttpServletRequest request, JSONObject fromOperator, String jobId) {
		Connection fromConnection = null;
		Connection currentConnection = null;
		PreparedStatement preparedStatement = null;
		try {
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String tableName = (String) fromOperator.get("tableName");
			if (tableName != null && tableName.contains(".")) {
				tableName = tableName.split("\\.")[1];
			}
			fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			List columnsObj = componentUtilities.getTableColumnsOpt(fromConnection, fromConnObj, tableName);
			List fromColumnsList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[2])
					.collect(Collectors.toList());
			String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column)
					.collect(Collectors.joining(","));
			Map tableColsObj = new LinkedHashMap();
			tableColsObj.put(tableName, fromColumnsListStr);
			String query = "SELECT * FROM " + tableName;
			List fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			String stagingTableName = "ZZ_TEMP_" + AuditIdGenerator.genRandom32Hex();

			String createTableQuery = "CREATE TABLE " + stagingTableName + "( ";
			createTableQuery += columnsObj.stream().map(rowData -> {
				String columnName = String.valueOf(((Object[]) rowData)[2]);
				if (columnName.contains("/")) {
					columnName = "\"" + columnName + "\"";
				}
				columnName = columnName + " " + ((Object[]) rowData)[8];
				// columnName = columnName + " " + "VARCHAR2(4000)";
				return columnName;
			}).collect(Collectors.joining(","));
			createTableQuery += " )";
			currentConnection = componentUtilities.getCurrentConnection();
			preparedStatement = currentConnection.prepareStatement(createTableQuery);
			preparedStatement.execute();

			((List) request.getAttribute("stagingTablesList")).add(stagingTableName);
			request.setAttribute("stagingTablesList", request.getAttribute("stagingTablesList"));

			String insertQuery = generateInsertQuery(stagingTableName, fromColumnsList);
			preparedStatement = currentConnection.prepareStatement(insertQuery);
			JSONObject infoObject = new JSONObject();
			// infoObject.put("skipRejectedRecords", skipRejectedRecords);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement,
					fromColumnsList, fromDataList, jobId, infoObject);
			int insertCount = (int) infoObject.get("insertCount");
			fromOperator.put("originalTableName", fromOperator.get("tableName"));
			fromOperator.put("tableName", stagingTableName);
			fromOperator.put("statusLabel", stagingTableName);
			fromOperator.put("CONN_CUST_COL1", "DB");
			fromOperator.put("connObj",
					new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10"));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
//            if (sapConnection != null) {
//                sapConnection.close();
//            }
				if (currentConnection != null) {
					currentConnection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fromOperator;
	}

	public JSONObject fileToLocalTable(HttpServletRequest request, JSONObject fromOperator, String jobId) {

		Connection currentConnection = null;
		PreparedStatement preparedStatement = null;
		try {
			JSONObject fileConnObj = (JSONObject) fromOperator.get("connObj");
			String fileName = (String) fileConnObj.get("fileName");
			String filePath = (String) fileConnObj.get("filePath");
			String fileType = (String) fileConnObj.get("fileType");
			String skipRejectedRecords = (String) fromOperator.get("skipRejectedRecords");

			List fromColumnsList = componentUtilities.getHeadersOfImportedFile(request, filePath);
			fromColumnsList = componentUtilities.fileHeaderValidations(fromColumnsList);
//            List dataTypesList = componentUtilities.getHeaderDataTypesOfImportedFile(request, filePath);
//            List dataTypesList = (List) fromColumnsList.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());
			JSONObject dataTypesObj = (JSONObject) fromOperator.get("dataTypesObj");
			List dataTypesList = new ArrayList();
			if (dataTypesObj != null) {
				for (Object col : fromColumnsList) {
					JSONObject typeObj = (JSONObject) dataTypesObj.get(col);
					String datatype = (String) typeObj.get("datatype");
					String columnsize = String.valueOf(typeObj.get("columnsize"));
					if (columnsize != null && !"".equalsIgnoreCase(columnsize)) {
						datatype = datatype + "(" + columnsize + ")";
					}
					dataTypesList.add(datatype);
				}
			} else {
//                dataTypesList = (List) fromColumnsList.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());
				dataTypesList = componentUtilities.getHeaderDataTypesOfImportedFile(request, filePath);
			}

			String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column)
					.collect(Collectors.joining(","));
			List<String> fromDataList = new ArrayList();
			if (fileType != null) {
				fileType = fileType.replaceAll("\\.", "");
			}
			if (fileType != null && !"null".equalsIgnoreCase(fileType)
					&& ("XLSX".equalsIgnoreCase(fileType) || "XLS".equalsIgnoreCase(fileType))) {
				fromDataList = fileProcessJobService.readExcelFile(request, filePath, fileName);
			} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && ("CSV".equalsIgnoreCase(fileType)
					|| "TXT".equalsIgnoreCase(fileType) || "JSON".equalsIgnoreCase(fileType))) {
				fromDataList = fileProcessJobService.readCSVFile(request, filePath, fileName, fileType,
						fromColumnsList);
			} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "XML".equalsIgnoreCase(fileType)) {
				fromDataList = fileProcessJobService.readXMLFile(request, filePath, fileName);
			} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "PDF".equalsIgnoreCase(fileType)) {
				fromDataList = fileProcessJobService.readPDF(request, filePath, fileName);
			}
			String stagingTableName = "ZZ_TEMP_" + AuditIdGenerator.genRandom32Hex();

			String createTableQuery = "CREATE TABLE " + stagingTableName + "( ";
			for (int i = 0; i < fromColumnsList.size(); i++) {

				String columnName = String.valueOf(fromColumnsList.get(i));
				if (columnName.contains("/")) {
					columnName = "\"" + columnName + "\"";
				}

				if (i < (fromColumnsList.size() - 1)) {
					createTableQuery += columnName + " " + dataTypesList.get(i) + " , ";
				} else {
					createTableQuery += columnName + " " + dataTypesList.get(i) + "";
				}

			}
//            createTableQuery += fromColumnsList.stream().map(column -> column + " VARCHAR2(4000) ").collect(Collectors.joining(","));
			createTableQuery += " )";
			currentConnection = componentUtilities.getCurrentConnection();
			preparedStatement = currentConnection.prepareStatement(createTableQuery);
			preparedStatement.execute();

			((List) request.getAttribute("stagingTablesList")).add(stagingTableName);
			request.setAttribute("stagingTablesList", request.getAttribute("stagingTablesList"));

			String insertQuery = generateInsertQuery(stagingTableName, fromColumnsList);
			preparedStatement = currentConnection.prepareStatement(insertQuery);
			JSONObject infoObject = new JSONObject();
			infoObject.put("skipRejectedRecords", skipRejectedRecords);
//            int insertCount = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement, fromColumnsList, fromDataList, jobId);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement,
					fromColumnsList, fromDataList, fromColumnsList, dataTypesList, "ORACLE", jobId, infoObject);
			int insertCount = (int) infoObject.get("insertCount");
			fromOperator.put("originalTableName", fileName.replaceAll("[^a-zA-Z0-9]", "_"));
			fromOperator.put("tableName", stagingTableName);
			fromOperator.put("statusLabel", stagingTableName);
			fromOperator.put("CONN_CUST_COL1", "DB");
			fromOperator.put("columnsList", fromColumnsList);
			fromOperator.put("connObj",
					new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10"));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
//            if (sapConnection != null) {
//                sapConnection.close();
//            }
				if (currentConnection != null) {
					currentConnection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fromOperator;
	}

	public JSONObject sapToTargetDBTable(HttpServletRequest request, JSONObject fromOperator, JSONObject toConnObj,
			String jobId) {
		JCO.Client sapConnection = null;
		Connection toConnection = null;
		PreparedStatement preparedStatement = null;
		try {
			JSONObject sapConnObj = (JSONObject) fromOperator.get("connObj");
			String tableName = (String) fromOperator.get("tableName");
			sapConnection = (JCO.Client) componentUtilities.getConnection(sapConnObj);
			List columnsObj = erpProcessJobService.getSAPTableColumnsWithType(request, sapConnection, tableName);
			List fromColumnsList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[2])
					.collect(Collectors.toList());
			List<Object[]> targetDataTypes = componentUtilities.getTargetDataType("SAP_HANA", "ORACLE");
			JSONObject fullDataTypeConvObject = new JSONObject();
			JSONObject dataTypeConvObject = new JSONObject();
			targetDataTypes.stream().forEach(rowData -> fullDataTypeConvObject.put(rowData[6],
					rowData[1] + (rowData[2] != null ? ("(" + rowData[2] + ")") : "")));

			String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column)
					.collect(Collectors.joining(","));
			Map tableColsObj = new LinkedHashMap();
			tableColsObj.put(tableName, fromColumnsListStr);
			List fromDataList = erpProcessJobService.getErpSelectedColumnsDataHib(tableColsObj, new JSONObject(),
					sapConnection, sapConnObj, 0, 10000);

			String stagingTableName = "ZZ_TEMP_" + AuditIdGenerator.genRandom32Hex();

			String createTableQuery = "CREATE TABLE " + stagingTableName + "( ";
			createTableQuery += columnsObj.stream().map(rowData -> {
				String columnName = String.valueOf(((Object[]) rowData)[2]);
				if (columnName.contains("/")) {
					columnName = "\"" + columnName + "\"";
				}
				columnName = columnName + " " + fullDataTypeConvObject.get(((Object[]) rowData)[3]);
				return columnName;
			}).collect(Collectors.joining(","));
			createTableQuery += " )";
			toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			preparedStatement = toConnection.prepareStatement(createTableQuery);
			preparedStatement.execute();

			List toColsList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[2])
					.collect(Collectors.toList());
			List toDataTypesList = (List) columnsObj.stream()
					.map(rowData -> dataTypeConvObject.get(((Object[]) rowData)[3])).collect(Collectors.toList());

			request.setAttribute("toConnStagingTable", stagingTableName);

			String insertQuery = generateInsertQuery(stagingTableName, fromColumnsList);
			preparedStatement = toConnection.prepareStatement(insertQuery);
			JSONObject infoObject = new JSONObject();
			// infoObject.put("skipRejectedRecords", skipRejectedRecords);
//            int insertCount = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement, fromColumnsList, fromDataList);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement,
					fromColumnsList, fromDataList, toColsList, toDataTypesList, "SAP_ECC", jobId, infoObject);
			int insertCount = (int) infoObject.get("insertCount");
			fromOperator.put("originalTableName", fromOperator.get("tableName"));
			fromOperator.put("tableName", stagingTableName);
			fromOperator.put("statusLabel", stagingTableName);
			fromOperator.put("CONN_CUST_COL1", "DB");
			fromOperator.put("connObj", toConnObj);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
//            if (sapConnection != null) {
//                sapConnection.close();
//            }
				if (toConnection != null) {
					toConnection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fromOperator;
	}

	public JSONObject otherDBToTargetDBTable(HttpServletRequest request, JSONObject fromOperator, JSONObject toConnObj,
			String jobId) {
		Connection fromConnection = null;
		Connection toConnection = null;
		PreparedStatement preparedStatement = null;
		try {
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromDBType = (String) fromConnObj.get("CONN_CUST_COL1");
			String tableName = (String) fromOperator.get("tableName");
			if (tableName != null && tableName.contains(".")) {
				tableName = tableName.split("\\.")[1];
			}
			fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			List columnsObj = componentUtilities.getTableColumnsOpt(fromConnection, fromConnObj, tableName);
			List fromColumnsList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[2])
					.collect(Collectors.toList());
			List fromColumnsDataTypesList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[3])
					.collect(Collectors.toList());
			String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column)
					.collect(Collectors.joining(","));
			Map tableColsObj = new LinkedHashMap();
			tableColsObj.put(tableName, fromColumnsListStr);
			String query = "SELECT * FROM " + tableName;
			List fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			String stagingTableName = "ZZ_TEMP_" + AuditIdGenerator.genRandom32Hex();

			String createTableQuery = "CREATE TABLE " + stagingTableName + "( ";
			createTableQuery += columnsObj.stream().map(rowData -> {
				String columnName = String.valueOf(((Object[]) rowData)[2]);
				if (columnName.contains("/")) {
					columnName = "\"" + columnName + "\"";
				}
				columnName = columnName + " " + ((Object[]) rowData)[8];
				return columnName;
			}).collect(Collectors.joining(","));
			createTableQuery += " )";
			System.out.println("createTableQuery :: " + createTableQuery);
			toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			preparedStatement = toConnection.prepareStatement(createTableQuery);
			preparedStatement.execute();

			request.setAttribute("toConnStagingTable", stagingTableName);

			String insertQuery = generateInsertQuery(stagingTableName, fromColumnsList);
			preparedStatement = toConnection.prepareStatement(insertQuery);
			JSONObject infoObject = new JSONObject();
			// infoObject.put("skipRejectedRecords", skipRejectedRecords);

//			infoObject = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement,
//					fromColumnsList, fromDataList, jobId, infoObject);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement,
					fromColumnsList, fromDataList, fromColumnsList, fromColumnsDataTypesList, fromDBType, jobId,
					infoObject);
			int insertCount = (int) infoObject.get("insertCount");
			fromOperator.put("originalTableName", fromOperator.get("tableName"));
			fromOperator.put("tableName", stagingTableName);
			fromOperator.put("statusLabel", stagingTableName);
			fromOperator.put("CONN_CUST_COL1", "DB");
			fromOperator.put("connObj", toConnObj);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
//            if (sapConnection != null) {
//                sapConnection.close();
//            }
				if (toConnection != null) {
					toConnection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fromOperator;
	}

	public JSONObject fileToTargetDBTable(HttpServletRequest request, JSONObject fromOperator, JSONObject toConnObj,
			String jobId) {

		Connection toConnection = null;
		PreparedStatement preparedStatement = null;
		try {
			JSONObject fileConnObj = (JSONObject) fromOperator.get("connObj");
			String fileName = (String) fileConnObj.get("fileName");
			String filePath = (String) fileConnObj.get("filePath");
			String fileType = (String) fileConnObj.get("fileType");

			List fromColumnsList = componentUtilities.getHeadersOfImportedFile(request, filePath);
			fromColumnsList = componentUtilities.fileHeaderValidations(fromColumnsList);
//            List dataTypesList = componentUtilities.getHeaderDataTypesOfImportedFile(request, filePath);
//            List dataTypesList = (List) fromColumnsList.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());

			JSONObject dataTypesObj = (JSONObject) fromOperator.get("dataTypesObj");
			List dataTypesList = new ArrayList();
			if (dataTypesObj != null) {
				for (Object col : fromColumnsList) {
					JSONObject typeObj = (JSONObject) dataTypesObj.get(col);
					String datatype = (String) typeObj.get("datatype");
					String columnsize = String.valueOf(typeObj.get("columnsize"));
					if (columnsize != null && !"".equalsIgnoreCase(columnsize)) {
						datatype = datatype + "(" + columnsize + ")";
					}
					dataTypesList.add(datatype);
				}
			} else {
//                dataTypesList = (List) fromColumnsList.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());
				dataTypesList = componentUtilities.getHeaderDataTypesOfImportedFile(request, filePath);
			}
			String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column)
					.collect(Collectors.joining(","));
			List<String> fromDataList = new ArrayList();
			if (fileType != null) {
				fileType = fileType.replaceAll("\\.", "");
			}
			if (fileType != null && !"null".equalsIgnoreCase(fileType)
					&& ("XLSX".equalsIgnoreCase(fileType) || "XLS".equalsIgnoreCase(fileType))) {
				fromDataList = fileProcessJobService.readExcelFile(request, filePath, fileName);
			} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && ("CSV".equalsIgnoreCase(fileType)
					|| "TXT".equalsIgnoreCase(fileType) || "JSON".equalsIgnoreCase(fileType))) {
				fromDataList = fileProcessJobService.readCSVFile(request, filePath, fileName, fileType,
						fromColumnsList);
			} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "XML".equalsIgnoreCase(fileType)) {
				fromDataList = fileProcessJobService.readXMLFile(request, filePath, fileName);
			}
			String stagingTableName = "ZZ_TEMP_" + AuditIdGenerator.genRandom32Hex();

			String createTableQuery = "CREATE TABLE " + stagingTableName + "( ";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String columnName = String.valueOf(fromColumnsList.get(i));
				if (columnName.contains("/")) {
					columnName = "\"" + columnName + "\"";
				}
				if (i < (fromColumnsList.size() - 1)) {
					createTableQuery += columnName + " " + dataTypesList.get(i) + " , ";
				} else {
					createTableQuery += columnName + " " + dataTypesList.get(i) + "";
				}

			}
//            createTableQuery += fromColumnsList.stream().map(column -> column + " VARCHAR2(4000) ").collect(Collectors.joining(","));
			createTableQuery += " )";
			toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			preparedStatement = toConnection.prepareStatement(createTableQuery);
			preparedStatement.execute();

			request.setAttribute("toConnStagingTable", stagingTableName);

			String insertQuery = generateInsertQuery(stagingTableName, fromColumnsList);
			preparedStatement = toConnection.prepareStatement(insertQuery);
			JSONObject infoObject = new JSONObject();
			// infoObject.put("skipRejectedRecords", skipRejectedRecords);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement,
					fromColumnsList, fromDataList, jobId, infoObject);
			int insertCount = (int) infoObject.get("insertCount");
			fromOperator.put("originalTableName", fileName.replaceAll("[^a-zA-Z0-9]", "_"));
			fromOperator.put("tableName", stagingTableName);
			fromOperator.put("statusLabel", stagingTableName);
			fromOperator.put("CONN_CUST_COL1", "DB");
			fromOperator.put("connObj", toConnObj);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
//            if (sapConnection != null) {
//                sapConnection.close();
//            }
				if (toConnection != null) {
					toConnection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fromOperator;
	}

	public Map processNonComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		String skipRejectedRecords = "";
		String truncateDestination = "";
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			JSONObject trfmRulesDataMap = (JSONObject) tooperator.get("trfmRules");
			String toTableName = (String) tooperator.get("tableName");
			if (toTableName != null && toTableName.contains(".")) {
				toTableName = toTableName.split("\\.")[1];
			}

			String subJobId = (String) tooperator.get("subJobId");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);

			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			String fromDBType = String.valueOf(fromConnObj.get("CONN_CUST_COL1"));
			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);

				} else {
					// db
				}
			} else {
				// files
				if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
					skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				}
				fromOperator.put("skipRejectedRecords", skipRejectedRecords);
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			String fromTable = (String) fromOperator.get("tableName");
			String OriginalTableName = (String) fromOperator.get("tableName");

			
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {

				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				truncateDestination = (String) trfmRulesDataMap.get("truncateDestination");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn.contains("/")) {
						if (destinationColumn.contains(":")) {
							destinationColumn = destinationColumn.split(":")[0] + ":" + "\""
									+ destinationColumn.split(":")[1] + "\"";
						} else {
							destinationColumn = "\"" + destinationColumn.split(":")[1] + "\"";
						}
					}

					if (destinationColumn != null && !"null".equalsIgnoreCase(destinationColumn)
							&& destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)
							&& !"null".equalsIgnoreCase(sourceColumn)) {

						if (sourceColumn.contains("/")) {
							if (sourceColumn.contains(":")) {
								sourceColumn = sourceColumn.split(":")[0] + ":" + "\"" + sourceColumn.split(":")[1]
										+ "\"";
							} else {
								sourceColumn = "\"" + sourceColumn.split(":")[1] + "\"";
							}
						}

						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)
							&& !"null".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
				}
			}

			// static code for O_RECORD_DATA_UNIFICATION_STG
//            if (toTableName != null && toTableName.contains("O_RECORD_DATA_UNIFICATION_STG")) {
//                String batchNumber = generation.get("AUTO_PROCESS", (String) request.getSession(false).getAttribute("ssOrgId"), "BATCH_ID");
//                batchNumber = "'" + batchNumber + "'";
//                if (toColumnsList.contains("BATCH_ID")) {
//                    int batchIdIndex = toColumnsList.indexOf("BATCH_ID");
//                    toColumnsList.remove(batchIdIndex);
//                    fromColumnsList.remove(batchIdIndex);
//                }
//                fromColumnsList.add(batchNumber);
//                toColumnsList.add("BATCH_ID");
//            }
			List fromColumnsAliasList = new ArrayList();
//            fromColumnsList = new ArrayList(fromColumnsSet);
//            toColumnsList = new ArrayList(toColumnsSet);

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
//				if (fromColumn.contains("/")) {
//					fromColumn = "\"" + fromColumn + "\"";
//				}
//				if (toColumn.contains("/")) {
//					toColumn = toColumn.replaceAll("/", "_");
//					if (toColumn.startsWith("_")) {
//						toColumn = toColumn.substring(1);
//					}
//				}
				if (i < (fromColumnsList.size() - 1)) {

					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}
			String query = "";
			if(columnsListStr != null && !"".equalsIgnoreCase(columnsListStr)) {
				query = "SELECT " + columnsListStr + " FROM " + fromTable;
			} else {
				query = "SELECT * FROM " + fromTable;
			}
			
			System.out.println("query :: " + query);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("CONNECTION_NAME")
					&& fromConnObj.containsKey("CONNECTION_NAME") && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))
					|| "Y".equalsIgnoreCase(String.valueOf(skipRejectedRecords))
					|| "Y".equalsIgnoreCase(String.valueOf(truncateDestination))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
					// sap code

					JSONObject sapFromColsObj = (JSONObject) componentUtilities.getColumnsObjFromQuery(query,
							(Connection) fromConnection, fromConnObj);
					List sapFromColsList = (List) sapFromColsObj.get("columnsList");
					List sapFromColsDataTypes = (List) sapFromColsObj.get("dataTypesList");
					if (toConnObj != null && !toConnObj.isEmpty()) {
						toConnection = componentUtilities.getConnection(toConnObj);

					}
					insertCount = erpProcessJobService.insertIntoSapTable(request, toTableName, fromDataList,
							(JCO.Client) toConnection, toColumnsList, sapFromColsDataTypes, jobId);
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"), "Inserted " + insertCount
										+ " Records in " + ((jobEndTime - jobStartTime) / 1000) + " Sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					// db

					if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
							&& fromConnObj.containsKey("HOST_NAME")
							&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
							&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
							&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
							&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
							&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))
							&& !"Y".equalsIgnoreCase(String.valueOf(skipRejectedRecords))
							&& !"Y".equalsIgnoreCase(String.valueOf(truncateDestination))) {
						if (toConnObj != null && !toConnObj.isEmpty()) {
							toConnection = componentUtilities.getConnection(toConnObj);
						}
						insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName,
								fromColumnsAliasList, toColumnsList, (Connection) toConnection, query, jobId);
						long jobEndTime = System.currentTimeMillis();
						long jobStartTime = (long) request.getAttribute("jobStartTime");

						try {
							componentUtilities.processETLReconciliationUpdateTarget(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
									String.valueOf(insertCount), subJobId, String.valueOf(toColumnsList.size()));
						} catch (Exception ex) {
						}

						try {
							componentUtilities
									.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											"Inserted " + insertCount + " Records into " + toTableName + " in "
													+ ((jobEndTime - jobStartTime) / 1000) + " Sec",
											"INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}
					} else {
						String insertQuery = generateInsertQuery(toTableName, toColumnsList);
						if (toConnObj != null && !toConnObj.isEmpty()) {
							toConnection = componentUtilities.getConnection(toConnObj);
						}

						List columnsObj = componentUtilities.getTableColumnsOpt((Connection) toConnection, toConnObj,
								toTableName);

						List toColumnsAllDataTypesList = (List) columnsObj.stream()
								.map(rowData -> ((Object[]) rowData)[3]).collect(Collectors.toList());
						Map columnNameDatatypeMap = new HashedMap();
						columnsObj.stream().forEach(
								rowData -> columnNameDatatypeMap.put(((Object[]) rowData)[2], ((Object[]) rowData)[3]));

						List toColumnsDataTypesList = (List) toColumnsList.stream()
								.map(column -> columnNameDatatypeMap.get(column)).collect(Collectors.toList());

						PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);
						if (truncateDestination != null
								&& "Y".equalsIgnoreCase(truncateDestination)) {
							String Truncatequery = "TRUNCATE TABLE " + toTableName + "";
							PreparedStatement truncatepreparedStatement = ((Connection) toConnection).prepareStatement(Truncatequery);
							truncatepreparedStatement.executeUpdate();
						}
						long startInsertTime = System.currentTimeMillis();
						JSONObject infoObject = new JSONObject();
						infoObject.put("toConnObj", toConnObj);
						infoObject.put("insertQuery", insertQuery);
						infoObject.put("skipRejectedRecords", skipRejectedRecords);
//						infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
//								preparedStatement, toColumnsList, fromDataList, jobId, infoObject);
						infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
								preparedStatement, toColumnsList, fromDataList, toColumnsList, toColumnsDataTypesList,
								fromDBType, jobId, infoObject);
						insertCount = (int) infoObject.get("insertCount");
						System.out.println(
								"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
						long jobEndTime = System.currentTimeMillis();
						long jobStartTime = (long) request.getAttribute("jobStartTime");
						if (toTableName.contains("ZZ_TEMP_TABLE")) {
							String abc = "";
						}

						if (skipRejectedRecords != null && "Y".equalsIgnoreCase(skipRejectedRecords)
								&& infoObject.get("rowByRowInsertCount") != null) {
							try {
								componentUtilities.processETLLog(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Inserted " + infoObject.get("rowByRowInsertCount") + " Records into "
												+ toTableName + " in " + ((jobEndTime - jobStartTime) / 1000) + " Sec",
										"INFO", 20, "Y", jobId);
							} catch (Exception ex) {
							}

							try {
								componentUtilities.processETLReconciliationUpdateTarget(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
										String.valueOf(infoObject.get("rowByRowInsertCount")), subJobId,
										String.valueOf(toColumnsList.size()));
							} catch (Exception ex) {
							}
							try {
								componentUtilities.processETLReconciliationUpdateRejectRecords(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"), jobId,
										String.valueOf(infoObject.get("rowByRowRejectCount")), subJobId);
							} catch (Exception ex) {
							}
						} else {

							try {
								componentUtilities.processETLReconciliationUpdateTarget(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
										String.valueOf(insertCount), subJobId, String.valueOf(toColumnsList.size()));
							} catch (Exception ex) {
							}

							try {
								componentUtilities.processETLLog(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Inserted " + insertCount + " Records into " + toTableName + " in "
												+ ((jobEndTime - jobStartTime) / 1000) + " Sec",
										"INFO", 20, "Y", jobId);
							} catch (Exception ex) {
							}
						}

					}

				}

			} else {
				// files

				String iconType = String.valueOf(tooperator.get("iconType"));
				String orginalName = (String) tooperator.get("userFileName");
				String filename = "";
				String message = "";
				String userName = (String) request.getSession(false).getAttribute("ssUsername");
				String filePath = etlFilePath + "ETL_EXPORT_" + File.separator + userName;

				String fileExt = String.valueOf(tooperator.get("iconType")).toLowerCase();
				String fileName = "V10ETLExport_" + tooperator.get("timeStamp") + "." + fileExt;
				filename = fileName;
				if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& ("XLSX".equalsIgnoreCase(iconType) || "XLS".equalsIgnoreCase(iconType))) {
					insertCount = fileProcessJobService.insertIntoXLSXFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& ("CSV".equalsIgnoreCase(iconType) || "TXT".equalsIgnoreCase(iconType))) {
					insertCount = fileProcessJobService.insertIntoTextOrCSVFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType) && "XML".equalsIgnoreCase(iconType)) {
					insertCount = fileProcessJobService.insertIntoXMLFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& "JSON".equalsIgnoreCase(iconType)) {
					insertCount = fileProcessJobService.insertIntoJSONFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				}

				long jobEndTime = System.currentTimeMillis();
				long jobStartTime = (long) request.getAttribute("jobStartTime");

				message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + filename + "','"
						+ orginalName + "') >Click here to download the " + iconType + " file</a>.";//

				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Inserted " + insertCount + " Records in " + ((jobEndTime - jobStartTime) / 1000) + " Sec",
							"INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}

				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), message, "INFO", 20, "Y",
							jobId);
				} catch (Exception ex) {
				}

			}

			try {
				componentUtilities.processETLJobPreview((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId,
						String.valueOf(tooperator.get("operatorId")), String.valueOf(insertCount));
			} catch (Exception ex) {
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processSapResumeComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		Connection currentConnection = null;
		int insertCount = 0;

		JSONObject toConnObj = new JSONObject();
		JSONObject fromConnObj = new JSONObject();
		String pkTable = "";
		String toTableName = "";
		String fromTable = "";
		List pkColumnsList = new ArrayList();
		List fromColumnsList = new ArrayList();
		List toColumnsList = new ArrayList();
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);
			pkTable = "ZZ_TEMPPK_" + String.valueOf(operator.get("timeStamp"));
			String dataCount = "";

			JSONArray toOperatorList = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			Object toOperatorId = (Object) toOperatorList.get(0);
			JSONObject toOperator = (JSONObject) operators.get(String.valueOf(toOperatorId));

			toConnObj = (JSONObject) toOperator.get("connObj");
			toTableName = (String) toOperator.get("tableName");
			if (toTableName != null && toTableName.contains(".")) {
				toTableName = toTableName.split("\\.")[1];
			}
//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			fromTable = (String) fromOperator.get("tableName");

			JSONObject trfmRulesDataMap = (JSONObject) toOperator.get("trfmRules");

//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();
			String skipRejectedRecords = "";
			String OriginalTableName = (String) fromOperator.get("tableName");
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
				}
			}
			toConnection = componentUtilities.getConnection(toConnObj);
			if (componentUtilities.checkTableExist(pkTable, (Connection) toConnection)) {
				int start = 0;
				int limit = 1000;
				List<Object[]> colsObjPk = componentUtilities.getTableColumnsOpt((Connection) toConnection, toConnObj,
						pkTable);
				pkColumnsList = colsObjPk.stream().map(colObjArray -> String.valueOf(((Object[]) colObjArray)[2]))
						.collect(Collectors.toList());
				String pkColumnsListStr = (String) pkColumnsList.stream().map(col -> col)
						.collect(Collectors.joining(","));

				List<Object[]> colsObj = componentUtilities.getTableColumnsOpt((Connection) toConnection, toConnObj,
						toTableName);
//                List toColumnsList = colsObj.stream().map(colObjArray -> String.valueOf(((Object[]) colObjArray)[2])).collect(Collectors.toList());
				String columnsListStr = (String) toColumnsList.stream().map(col -> col)
						.collect(Collectors.joining(","));

				erpProcessJobService.loadRemainingDataToTarget(request, pkTable, toTableName, pkColumnsList,
						fromColumnsList, toColumnsList, fromOperator, toOperator, jobId);

			} else {

				fromJCOConnection = (JCO.Client) componentUtilities.getConnection(fromConnObj);
				List sapColumnsObj = erpProcessJobService.getSAPTableColumnsWithType(request, fromJCOConnection,
						fromTable);
				pkColumnsList = (List) sapColumnsObj.stream()
						.filter(colObjArray -> "X".equalsIgnoreCase(String.valueOf(((Object[]) colObjArray)[9])))
						.map(colObjArray -> String.valueOf(((Object[]) colObjArray)[2])).collect(Collectors.toList());
				if (toConnection == null) {
					toConnection = componentUtilities.getConnection(toConnObj);
				}

				List fianlPkcols = new ArrayList(pkColumnsList);
				List<Object[]> colsObj = componentUtilities.getTableColumnsOpt((Connection) toConnection, toConnObj,
						toTableName);

				List pkColumnsDataTypes = colsObj.stream()
						.filter(colObjArray -> fianlPkcols.contains(String.valueOf(((Object[]) colObjArray)[2])))
						.map(colObjArray -> String.valueOf(((Object[]) colObjArray)[8])).collect(Collectors.toList());
				boolean tableCreated = componentUtilities.createTable(pkTable, pkColumnsList, pkColumnsDataTypes,
						(Connection) toConnection);
				JSONObject toPKOpObj = new JSONObject();
				toPKOpObj.put("connObj", toConnObj);
				toPKOpObj.put("tableName", pkTable);

				erpProcessJobService.loadSAPDataToTarget(request, pkColumnsList, pkColumnsList, fromOperator, toPKOpObj,
						jobId);

				erpProcessJobService.loadRemainingDataToTarget(request, pkTable, toTableName, pkColumnsList,
						fromColumnsList, toColumnsList, fromOperator, toOperator, jobId);

			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (toConnection == null) {
					toConnection = componentUtilities.getConnection(toConnObj);
				}
				if (fromJCOConnection == null) {
					fromJCOConnection = (JCO.Client) componentUtilities.getConnection(fromConnObj);
				}
				int fromTableCount = erpProcessJobService.getSapTableRowCount(request, fromJCOConnection,
						fromColumnsList, fromTable, jobId);
				int pkTableRowCount = componentUtilities.getTableCount(pkTable, (Connection) toConnection);

				int remainingrecordsCount = processJobComponentsDAO.getRemainingRecordsCount(request, pkColumnsList,
						pkTable, toTableName, (Connection) toConnection);

				if (remainingrecordsCount <= 0 || fromTableCount != pkTableRowCount) {
					componentUtilities.dropStagingTable(pkTable, (Connection) toConnection);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
				if (currentConnection != null) {
					((Connection) currentConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processSapJoinComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);

			JSONArray toOperatorList = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			Object toOperatorId = (Object) toOperatorList.get(0);
			JSONObject toOperator = (JSONObject) operators.get(String.valueOf(toOperatorId));
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			JSONObject trfmRulesDataMap = (JSONObject) toOperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			String fromTable = (String) fromOperator.get("tableName");
			String OriginalTableName = (String) fromOperator.get("tableName");

			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						if (sourceColumn.contains(":")) {
							sourceColumn = sourceColumn.replace(":", ".");
//                            sourceColumn = sourceColumn.split(":")[1]; 
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
				}
			}

			// static code for O_RECORD_DATA_UNIFICATION_STG
			List fromColumnsAliasList = new ArrayList();

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
				if (i < (fromColumnsList.size() - 1)) {
					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}
//            String query = "SELECT " + columnsListStr + " FROM " + fromTable;
//            System.out.println("query :: " + query);

//            String joinCondition = "";
			JSONObject joinTrfmRulesDataMap = (JSONObject) operator.get("trfmRules");
			String joinQuery = "";
			String childKeyColumn = "";
			String masterKeyColumn = "";
			List<String> joinClauseDataMapList = (List) joinTrfmRulesDataMap.get("joinClauseData");
			List<String> childTables = (List) joinTrfmRulesDataMap.get("childTables");
			if (childTables != null && !childTables.isEmpty() && joinClauseDataMapList != null
					&& !joinClauseDataMapList.isEmpty()) {
				JSONObject joinQueryMapObj = new JSONObject();
				LinkedHashMap joinQueryHashMapObj = new LinkedHashMap();

				String masterTableName = (String) joinTrfmRulesDataMap.get("masterTableName");
				joinQuery += " " + masterTableName;
				joinQueryMapObj.put(masterTableName, masterTableName);
				joinQueryHashMapObj.put(masterTableName, masterTableName);
				for (int i = 0; i < childTables.size(); i++) {
					String childTableName = childTables.get(i);
					String childJoinStr = joinClauseDataMapList.get(i);
					if (childJoinStr != null && !"".equalsIgnoreCase(childJoinStr)
							&& !"null".equalsIgnoreCase(childJoinStr)) {
						JSONObject joinObj = (JSONObject) JSONValue.parse(childJoinStr);
						if (joinObj != null && !joinObj.isEmpty()) {
							joinQueryMapObj.put(childTableName, joinObj);
							joinQueryHashMapObj.put(childTableName, joinObj);
							int j = 0;
							for (Object joinObjKey : joinObj.keySet()) {
								JSONObject joinMappedColumnObj = (JSONObject) joinObj.get(joinObjKey);
								if (joinMappedColumnObj != null && !joinMappedColumnObj.isEmpty()) {
									String childTableColumn = "";
									if (joinMappedColumnObj.get("childTableColumn") != null
											&& !"".equalsIgnoreCase(
													String.valueOf(joinMappedColumnObj.get("childTableColumn")))
											&& !"null".equalsIgnoreCase(
													String.valueOf(joinMappedColumnObj.get("childTableColumn")))) {// childTableColumn
//                                        childTableColumn = String.valueOf(joinMappedColumnObj.get("childTableColumn"));
										childTableColumn = String
												.valueOf(joinMappedColumnObj.get("childTableColumnActualValue"));
										childTableColumn = childTableColumn.replace(":", "~");
										childKeyColumn = childTableColumn.replace("~", ".");
									}
									String masterTableColumn = "";
									if (joinMappedColumnObj.get("masterTableColumn") != null
											&& !"".equalsIgnoreCase(
													String.valueOf(joinMappedColumnObj.get("masterTableColumn")))
											&& !"null".equalsIgnoreCase(
													String.valueOf(joinMappedColumnObj.get("masterTableColumn")))) {// childTableColumn
//                                        masterTableColumn = String.valueOf(joinMappedColumnObj.get("masterTableColumn"));
										masterTableColumn = String
												.valueOf(joinMappedColumnObj.get("masterTableColumnActualValue"));
										masterTableColumn = masterTableColumn.replace(":", "~");
										masterKeyColumn = masterTableColumn.replace("~", ".");
									}
									if (j == 0) {
										joinQuery += " " + joinMappedColumnObj.get("joinType") + " " + childTableName
												+ " ON ";
									}
									String colValue = String.valueOf(joinMappedColumnObj.get("staticValue"));
									if (colValue != null && !"".equalsIgnoreCase(colValue)
											&& !"null".equalsIgnoreCase(colValue)) {
										if (joinMappedColumnObj.get("operator") != null
												&& !"".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("operator")))
												&& !"null".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("operator")))
												&& ("IN".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("operator")))
														|| "NOT IN".equalsIgnoreCase(
																String.valueOf(joinMappedColumnObj.get("operator"))))) {
											colValue = "('" + colValue.replaceAll("##", "','") + "')";
										} else {
											colValue = "'" + colValue + "'";
										}

									}

									joinQuery += " " + childTableColumn + " " + joinMappedColumnObj.get("operator")
											+ " " + " "
											+ ((joinMappedColumnObj.get("staticValue") != null
													&& !"".equalsIgnoreCase(
															String.valueOf(joinMappedColumnObj.get("staticValue")))
													&& !"null".equalsIgnoreCase(
															String.valueOf(joinMappedColumnObj.get("staticValue"))))
																	? "" + colValue + ""
																	: masterTableColumn);
									if (j != joinObj.size() - 1) {
										joinQuery += " AND ";
									}
								}
								j++;
							}
						}
					}
				}
			}

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					erpProcessJobService.loadSAPJoinsDataToTarget(request, fromColumnsList, toColumnsList, fromOperator,
							toOperator, joinQuery, childKeyColumn, masterKeyColumn, jobId);

				} else {
					// db
//                    fromDataList = erpProcessJobService.loadTTableDataToTarget(request, fromColumnsList, toColumnsList, fromOperator, toOperator, jobId);

				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processOneToOneComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);

			JSONArray toOperatorList = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			Object toOperatorId = (Object) toOperatorList.get(0);
			JSONObject toOperator = (JSONObject) operators.get(String.valueOf(toOperatorId));
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			JSONObject trfmRulesDataMap = (JSONObject) toOperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			String fromTable = (String) fromOperator.get("tableName");
			String OriginalTableName = (String) fromOperator.get("tableName");

			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
				}
			}

			// static code for O_RECORD_DATA_UNIFICATION_STG
			List fromColumnsAliasList = new ArrayList();

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
				if (i < (fromColumnsList.size() - 1)) {
					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}
//            String query = "SELECT " + columnsListStr + " FROM " + fromTable;
//            System.out.println("query :: " + query);

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {

					erpProcessJobService.loadSAPDataToTarget(request, fromColumnsList, toColumnsList, fromOperator,
							toOperator, jobId);

				} else {

				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processOneToOneComponentJobStandard(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);

			JSONArray toOperatorList = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			Object toOperatorId = (Object) toOperatorList.get(0);
			JSONObject toOperator = (JSONObject) operators.get(String.valueOf(toOperatorId));
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			JSONObject trfmRulesDataMap = (JSONObject) toOperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			String fromTable = (String) fromOperator.get("tableName");
			String OriginalTableName = (String) fromOperator.get("tableName");

			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
				}
			}

			// static code for O_RECORD_DATA_UNIFICATION_STG
			List fromColumnsAliasList = new ArrayList();

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
				if (i < (fromColumnsList.size() - 1)) {
					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}
//            String query = "SELECT " + columnsListStr + " FROM " + fromTable;
//            System.out.println("query :: " + query);

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {

					erpProcessJobService.loadSAPDataToTargetStandard(request, fromColumnsList, toColumnsList,
							fromOperator, toOperator, jobId);

				} else {

				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processOneToOneComponentJobReverse(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);

			JSONArray toOperatorList = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			Object toOperatorId = (Object) toOperatorList.get(0);
			JSONObject toOperator = (JSONObject) operators.get(String.valueOf(toOperatorId));
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			JSONObject trfmRulesDataMap = (JSONObject) toOperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			String fromTable = (String) fromOperator.get("tableName");
			String OriginalTableName = (String) fromOperator.get("tableName");

			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
				}
			}

			// static code for O_RECORD_DATA_UNIFICATION_STG
			List fromColumnsAliasList = new ArrayList();

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
				if (i < (fromColumnsList.size() - 1)) {
					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}
//            String query = "SELECT " + columnsListStr + " FROM " + fromTable;
//            System.out.println("query :: " + query);

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {

					erpProcessJobService.loadSAPDataToTargetReverse(request, fromColumnsList, toColumnsList,
							fromOperator, toOperator, jobId);

				} else {

				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	// public Map processResumeComponentJob(HttpServletRequest request, String
	// operatorId, Map operators, JSONObject flowchartData, String jobId) {
//
//        JSONObject resultObj = new JSONObject();
//        Object fromConnection = null;
//        Object toConnection = null;
//        JCO.Client fromJCOConnection = null;
//        JCO.Client toJCOConnection = null;
//        int insertCount = 0;
//        try {
//              JSONObject tooperator = (JSONObject) operators.get(operatorId);
//            String toTableName = (String) tooperator.get("tableName");
//            JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
//            if (toConnObj != null && !toConnObj.isEmpty()) {
//                toConnection = componentUtilities.getConnection(toConnObj);
//            }
//
////            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
//            JSONArray fromOperatorList = getConnectedFromOperatorIds(request, String.valueOf(tooperator.get("operatorId")), flowchartData);
//            Object fromOperatorId = (Object) fromOperatorList.get(0);
//            JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
//            JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
//            
//           JSONObject operator = (JSONObject) operators.get(operatorId);
//           String pkTable = (String)operator.get("pkColumnsTable");
//           String pkColumns = "MANDT";
//            S
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//
//            try {
//                if (fromConnection != null) {
//                    ((Connection) fromConnection).close();
//                }
//                if (toConnection != null) {
//                    ((Connection) toConnection).close();
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        return operators;
//    }
//
//    
	public Map processOutputComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);

				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRulesDataMap = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			String fromTable = (String) fromOperator.get("tableName");

			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						fromColumnsList.addAll(funcolumnslist);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
					}

					toColumnsList.add(destinationColumn);
				}
			}

			List fromColumnsAliasList = new ArrayList();
//            fromColumnsList = new ArrayList(fromColumnsSet);
//            toColumnsList = new ArrayList(toColumnsSet);

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
				if (i < (fromColumnsList.size() - 1)) {
					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}
			String query = "SELECT " + columnsListStr + " FROM " + fromTable;
			System.out.println("query :: " + query);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))
					|| "Y".equalsIgnoreCase(String.valueOf(skipRejectedRecords))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			}

			// files
//                String iconType = String.valueOf(tooperator.get("iconType"));
			String iconType = "XLSX";
			String orginalName = (String) tooperator.get("userFileName");
			String filename = "";
			String message = "";
			String userName = (String) request.getSession(false).getAttribute("ssUsername");
			String filePath = etlFilePath + "ETL_EXPORT_" + File.separator + userName;

			String fileExt = iconType.toLowerCase();
			String fileName = "V10ETLExport_" + tooperator.get("timeStamp") + "." + fileExt;
			filename = fileName;
			insertCount = fileProcessJobService.insertIntoXLSXFile(request, tooperator, toColumnsList, fromDataList,
					filePath, fileName);

			long jobEndTime = System.currentTimeMillis();
			long jobStartTime = (long) request.getAttribute("jobStartTime");

			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"),
						"Inserted " + insertCount + " Records in " + ((jobEndTime - jobStartTime) / 1000) + " Sec",
						"INFO", 20, "Y", jobId);
			} catch (Exception ex) {
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processMergeFilesComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);

			for (int i = 0; i < fromOperatorList.size(); i++) {
				Object fromOperatorId = (Object) fromOperatorList.get(i);
				JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
				JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

				fromOperator = fileToLocalTable(request, fromOperator, jobId);
				List<String> columnsList = (List) fromOperator.get("columnsList");
				String fromTable = (String) fromOperator.get("tableName");
				if (i == 0) {
					// fromConnection = componentUtilities.getConnection(fromConnObj);

					String query = "CREATE TABLE " + toTableName + " AS SELECT * FROM " + fromTable;
					componentUtilities.createTableWithQuery(query, (Connection) toConnection);
				} else {
					String insertValuesCond = columnsList.stream().map(col -> "SRC." + col)
							.collect(Collectors.joining(", "));
					String insertColumns = columnsList.stream().map(col -> "DEST." + col)
							.collect(Collectors.joining(", "));
					String mergeQueryInsert = "MERGE INTO " + toTableName + " DEST USING " + " ( SELECT * FROM "
							+ fromTable + " ) SRC " + " ON (1=2)  WHEN NOT MATCHED THEN " + " INSERT (" + insertColumns
							+ ") VALUES (" + insertValuesCond + ")";
					System.out.println("Merge Query -->>" + mergeQueryInsert);
					PreparedStatement stmt = ((Connection) toConnection).prepareStatement(mergeQueryInsert);
					insertCount = stmt.executeUpdate();

				}
				operators.put(String.valueOf(fromOperatorId), fromOperator);

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processStagingComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);

				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRulesDataMap = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			String fromTable = (String) fromOperator.get("tableName");
			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						fromColumnsList.addAll(funcolumnslist);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
					}

					toColumnsList.add(destinationColumn);
				}
			}

			List fromColumnsAliasList = new ArrayList();
//            fromColumnsList = new ArrayList(fromColumnsSet);
//            toColumnsList = new ArrayList(toColumnsSet);

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
				if (i < (fromColumnsList.size() - 1)) {
					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}
			String query = "SELECT " + columnsListStr + " FROM " + fromTable;
			System.out.println("query :: " + query);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))
					|| "Y".equalsIgnoreCase(String.valueOf(skipRejectedRecords))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			}
			String colObjquery = "SELECT " + columnsListStr + " FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery,
					(Connection) fromConnection, fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;
			boolean tableCreated = componentUtilities.createTable(toTableName, columnsList, dataTypesList,
					(Connection) toConnection);

			// db
			if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))
					&& !"Y".equalsIgnoreCase(String.valueOf(skipRejectedRecords))) {

				insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsAliasList,
						toColumnsList, (Connection) toConnection, query, jobId);
				long jobEndTime = System.currentTimeMillis();
				long jobStartTime = (long) request.getAttribute("jobStartTime");
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Processed " + insertCount + " Records  ", "INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}
			} else {
				String insertQuery = generateInsertQuery(toTableName, toColumnsList);
				PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

				long startInsertTime = System.currentTimeMillis();
				JSONObject infoObject = new JSONObject();
				infoObject.put("skipRejectedRecords", skipRejectedRecords);
				infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
						toColumnsList, fromDataList, jobId, infoObject);
				insertCount = (int) infoObject.get("insertCount");
				System.out.println("Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
				long jobEndTime = System.currentTimeMillis();
				long jobStartTime = (long) request.getAttribute("jobStartTime");
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Processed " + insertCount + " Records  ", "INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public JSONObject filterRejectedRecords(HttpServletRequest request, JSONObject filterOperator, Map operators,
			JSONObject flowchartData, String jobId) {
		JSONObject resultObj = new JSONObject();
		Connection toConnection = null;
		List fromDataList = new ArrayList();
		try {
//            JSONArray connectedFrom = (JSONArray) filterOperator.get("connectedFrom");
			JSONArray connectedFrom = getConnectedFromOperatorIds(request,
					String.valueOf(filterOperator.get("operatorId")), flowchartData);
			String fromOperatorId = String.valueOf(connectedFrom.get(0));
			JSONObject fromOperator = (JSONObject) operators.get(fromOperatorId);
			String fromTable = (String) fromOperator.get("statusLabel");
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			JSONObject trfmRules = (JSONObject) filterOperator.get("trfmRules");

			String whereClauseCond = "";
			JSONArray whereClauseData = (JSONArray) trfmRules.get("whereClauseData");
			for (int i = 0; i < whereClauseData.size(); i++) {
				String mappedDataStr = (String) whereClauseData.get(i);
				JSONObject mappedData = (JSONObject) JSONValue.parse(mappedDataStr);
				int index = 0;
				for (Object key : mappedData.keySet()) {
					index++;
					JSONObject filterInfo = (JSONObject) mappedData.get(key);
//                    String columnName = (String) filterInfo.get("columnName");
					String columnName = (String) filterInfo.get("columnNameActualValue");
					// columnName = columnName.replaceAll(fromTable + ":", fromTable + ".");

					if (columnName.contains(":")) {
						columnName = columnName.split(":")[1];
					}
					if (columnName.contains(".")) {
						columnName = columnName.split("\\.")[1];
					}

					String operator = (String) filterInfo.get("operator");
					if ("=".equalsIgnoreCase(operator)) {
						operator = "!=";
					} else if ("!=".equalsIgnoreCase(operator)) {
						operator = "=";
					} else if ("LIKE".equalsIgnoreCase(operator)) {
						operator = "NOT LIKE";
					} else if ("NOT LIKE".equalsIgnoreCase(operator)) {
						operator = "LIKE";
					} else if ("IN".equalsIgnoreCase(operator)) {
						operator = "NOT IN";
					} else if ("NOT IN".equalsIgnoreCase(operator)) {
						operator = "IN";
					} else if ("IS".equalsIgnoreCase(operator)) {
						operator = "NOT IS";
					} else if ("NOT IS".equalsIgnoreCase(operator)) {
						operator = "IS";
					} else if ("<".equalsIgnoreCase(operator)) {
						operator = ">=";
					} else if (">=".equalsIgnoreCase(operator)) {
						operator = "<";
					} else if (">".equalsIgnoreCase(operator)) {
						operator = "<=";
					} else if ("<=".equalsIgnoreCase(operator)) {
						operator = ">";
					}
					String staticValue = (String) filterInfo.get("staticValue");
					String andOrOperator = (String) filterInfo.get("andOrOperator");
					if (index == mappedData.size()) {
						whereClauseCond += columnName + " " + operator + " '" + staticValue + "' ";
					} else {
						whereClauseCond += columnName + " " + operator + " '" + staticValue + "' " + andOrOperator
								+ " ";
					}
				}
			}
			String query = "SELECT * FROM " + fromTable + " WHERE " + whereClauseCond;
			fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			resultObj.put("fromDataList", fromDataList);
			resultObj.put("query", query);
//             JSONObject tooperator = (JSONObject) operators.get(operatorId);
//            String toTableName = (String) tooperator.get("tableName");
//            JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
//            if (toConnObj != null && !toConnObj.isEmpty()) {
//                toConnection = (Connection)componentUtilities.getConnection(toConnObj);
//            }
//             if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
//                if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
//                    //sap code
//
//                    JSONObject sapFromColsObj = (JSONObject)componentUtilities.getColumnsObjFromQuery(query, (Connection)fromConnection, fromConnObj);
//                    List sapFromColsList = (List)sapFromColsObj.get("columnsList");
//                    List sapFromColsDataTypes = (List)sapFromColsObj.get("dataTypesList");
//                    insertCount = erpProcessJobService.insertIntoSapTable(request, toTableName, fromDataList, (JCO.Client) toConnection, toColumnsList, sapFromColsDataTypes, jobId);
//                    long jobEndTime = System.currentTimeMillis();
//                    long jobStartTime = (long) request.getAttribute("jobStartTime");
//                    try {
//                        componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
//                                (String) request.getSession(false).getAttribute("ssOrgId"),
//                                "Inserted " + insertCount + " Records in " + ((jobEndTime - jobStartTime) / 1000) + " Sec", "INFO", 20, "Y", jobId);
//                    } catch (Exception ex) {
//                    }
//                } else {
//                    // db
//
//                    String insertQuery = generateInsertQuery(toTableName, toColumnsList);
//                    PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);
//
//                    long startInsertTime = System.currentTimeMillis();
//                    insertCount = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement, toColumnsList, fromDataList, jobId);
//                    System.out.println("Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
//                    long jobEndTime = System.currentTimeMillis();
//                    long jobStartTime = (long) request.getAttribute("jobStartTime");
//                    try {
//                        componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
//                                (String) request.getSession(false).getAttribute("ssOrgId"),
//                                insertCount + " Records Rejected ", "INFO", 20, "Y", jobId);
//                    } catch (Exception ex) {
//                    }
//
//                }
//            } else {
//                // files
//
//                String iconType = String.valueOf(tooperator.get("iconType"));
//                String orginalName = (String) tooperator.get("userFileName");
//                String filename = "";
//                String message = "";
//                String userName = (String) request.getSession(false).getAttribute("ssUsername");
//                String filePath = "C://ETL_EXPORT_" + File.separator + userName;
//
//                String fileExt = String.valueOf(tooperator.get("iconType")).toLowerCase();
//                String fileName = "V10ETLExport_" + tooperator.get("timeStamp") + "." + fileExt;
//                filename = fileName;
//                if (iconType != null && !"null".equalsIgnoreCase(iconType) && ("XLSX".equalsIgnoreCase(iconType) || "XLS".equalsIgnoreCase(iconType))) {
//                    insertCount = fileProcessJobService.insertIntoXLSXFile(request, tooperator, toColumnsList, fromDataList, filePath, fileName);
//                } else if (iconType != null && !"null".equalsIgnoreCase(iconType) && ("CSV".equalsIgnoreCase(iconType) || "TXT".equalsIgnoreCase(iconType))) {
//                    insertCount = fileProcessJobService.insertIntoTextOrCSVFile(request, tooperator, toColumnsList, fromDataList, filePath, fileName);
//                } else if (iconType != null && !"null".equalsIgnoreCase(iconType) && "XML".equalsIgnoreCase(iconType)) {
//                    insertCount = fileProcessJobService.insertIntoXMLFile(request, tooperator, toColumnsList, fromDataList, filePath, fileName);
//                } else if (iconType != null && !"null".equalsIgnoreCase(iconType) && "JSON".equalsIgnoreCase(iconType)) {
//                    insertCount = fileProcessJobService.insertIntoJSONFile(request, tooperator, toColumnsList, fromDataList, filePath, fileName);
//                }
//
//                long jobEndTime = System.currentTimeMillis();
//                long jobStartTime = (long) request.getAttribute("jobStartTime");
//
//                message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + filename + "','" + orginalName + "') >Click here to download the " + iconType + " file</a>.";//
//
//                try {
//                    componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
//                            (String) request.getSession(false).getAttribute("ssOrgId"),
//                            insertCount + " Records Rejected ", "INFO", 20, "Y", jobId);
//                } catch (Exception ex) {
//                }
//
//                try {
//                    componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
//                            (String) request.getSession(false).getAttribute("ssOrgId"),
//                            message, "INFO", 20, "Y", jobId);
//                } catch (Exception ex) {
//                }
//            }
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		return resultObj;
	}

	public JSONObject uniqueRejectedRecords(HttpServletRequest request, JSONObject uniqueOperator, Map operators,
			JSONObject flowchartData, String jobId) {
		Connection fromConnection = null;
		JSONObject resultObj = new JSONObject();
		List fromDataList = new ArrayList();
		try {
//            JSONArray connectedFrom = (JSONArray) uniqueOperator.get("connectedFrom");
			JSONArray connectedFrom = getConnectedFromOperatorIds(request,
					String.valueOf(uniqueOperator.get("operatorId")), flowchartData);
			String fromOperatorId = String.valueOf(connectedFrom.get(0));
			JSONObject fromOperator = (JSONObject) operators.get(fromOperatorId);
			String fromTable = (String) fromOperator.get("statusLabel");
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			JSONObject trfmRules = (JSONObject) uniqueOperator.get("trfmRules");
			JSONArray uniqueKeys = (JSONArray) trfmRules.get("uniqueKeys");
			processJobComponentsDAO.deleteUniqueRecords(request, fromTable, new ArrayList(uniqueKeys), fromConnection,
					jobId);
			String query = "SELECT * FROM " + fromTable;
			fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			resultObj.put("fromDataList", fromDataList);
			resultObj.put("query", query);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		return resultObj;
	}

	public JSONObject mergeRejectedRecords(HttpServletRequest request, JSONObject mergeOperator, Map operators,
			JSONObject flowchartData, String jobId) {
		Connection fromConnection = null;
		JSONObject resultObj = new JSONObject();
		List fromDataList = new ArrayList();
		try {

			JSONObject trfmRules = (JSONObject) mergeOperator.get("trfmRules");
			String operatorType = (String) trfmRules.get("operatorType");
			if (operatorType.equalsIgnoreCase("Insert")) {
				resultObj = mergeInsertRejectedRecords(request, mergeOperator, operators, flowchartData, jobId);
			} else if (operatorType.equalsIgnoreCase("Update")) {
				resultObj = mergeUpdateRejectedRecords(request, mergeOperator, operators, flowchartData, jobId);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		return resultObj;
	}

	public JSONObject mergeUpdateRejectedRecords(HttpServletRequest request, JSONObject mergeOperator, Map operators,
			JSONObject flowchartData, String jobId) {
		Connection fromConnection = null;
		JSONObject resultObj = new JSONObject();
		List fromDataList = new ArrayList();
		try {
//            JSONArray connectedFrom = (JSONArray) mergeOperator.get("connectedFrom");
			JSONArray connectedFrom = getConnectedFromOperatorIds(request,
					String.valueOf(mergeOperator.get("operatorId")), flowchartData);
			String fromOperatorId = String.valueOf(connectedFrom.get(0));
			JSONObject fromOperator = (JSONObject) operators.get(fromOperatorId);
			String fromTable = (String) fromOperator.get("statusLabel");
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);

//            JSONArray connectedTo = (JSONArray) mergeOperator.get("connectedTo");
			JSONArray connectedTo = getConnectedToOperatorIds(request, String.valueOf(mergeOperator.get("operatorId")),
					flowchartData);
//            String mergeTargetOperator = connectedTo.get(0);
//            JSONObject trfmRules = (JSONObject) uniqueOperator.get("trfmRules");
//            JSONArray uniqueKeys = (JSONArray) trfmRules.get("uniqueKeys");
//            processJobComponentsDAO.deleteUniqueRecords(request, fromTable, new ArrayList(uniqueKeys), fromConnection);
			String query = "SELECT * FROM " + fromTable;
			fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			resultObj.put("fromDataList", fromDataList);
			resultObj.put("query", query);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		return resultObj;
	}

	public JSONObject mergeInsertRejectedRecords(HttpServletRequest request, JSONObject mergeOperator, Map operators,
			JSONObject flowchartData, String jobId) {
		Connection fromConnection = null;
		JSONObject resultObj = new JSONObject();
		List fromDataList = new ArrayList();
		try {
//            JSONArray connectedFrom = (JSONArray) mergeOperator.get("connectedFrom");
			JSONArray connectedFrom = getConnectedFromOperatorIds(request,
					String.valueOf(mergeOperator.get("operatorId")), flowchartData);
			String fromOperatorId = String.valueOf(connectedFrom.get(0));
			JSONObject fromOperator = (JSONObject) operators.get(fromOperatorId);
			String fromTable = (String) fromOperator.get("statusLabel");
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
//            processJobComponentsDAO.deleteUniqueRecords(request, fromTable, new ArrayList(uniqueKeys), fromConnection);
			String query = "SELECT * FROM " + fromTable;
			fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			resultObj.put("fromDataList", fromDataList);
			resultObj.put("query", query);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		return resultObj;
	}

	public Map processRejectedComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object sourceConnection = null;
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {
			List fromDataList = new ArrayList();
			List toColumnsList = new ArrayList();
			String query = "";
			JSONObject operator = (JSONObject) operators.get(operatorId);
//            JSONArray fromOperatorList = (JSONArray) operator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			String fromIconType = (String) fromOperator.get("iconType");
			if (fromIconType != null && "FILTER".equalsIgnoreCase(fromIconType)) {
				JSONObject rejectRecordsObj = filterRejectedRecords(request, fromOperator, operators, flowchartData,
						jobId);
				fromDataList = (List) rejectRecordsObj.get("fromDataList");
				query = (String) rejectRecordsObj.get("query");
				toColumnsList = (List) fromOperator.get("simpleColumnsList");
			} else if (fromIconType != null && "UNIQUE".equalsIgnoreCase(fromIconType)) {
				JSONObject rejectRecordsObj = uniqueRejectedRecords(request, fromOperator, operators, flowchartData,
						jobId);
				fromDataList = (List) rejectRecordsObj.get("fromDataList");
				query = (String) rejectRecordsObj.get("query");
				toColumnsList = (List) fromOperator.get("simpleColumnsList");
			} else if (fromIconType != null && "MERGE".equalsIgnoreCase(fromIconType)) {
				JSONObject rejectRecordsObj = mergeRejectedRecords(request, fromOperator, operators, flowchartData,
						jobId);
				fromDataList = (List) rejectRecordsObj.get("fromDataList");
				query = (String) rejectRecordsObj.get("query");
				toColumnsList = (List) fromOperator.get("simpleColumnsList");
			}
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRulesDataMap = (JSONObject) operator.get("trfmRules");

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
					// sap code

					JSONObject sapFromColsObj = (JSONObject) componentUtilities.getColumnsObjFromQuery(query,
							(Connection) fromConnection, fromConnObj);
					List sapFromColsList = (List) sapFromColsObj.get("columnsList");
					List sapFromColsDataTypes = (List) sapFromColsObj.get("dataTypesList");
					insertCount = erpProcessJobService.insertIntoSapTable(request, toTableName, fromDataList,
							(JCO.Client) toConnection, toColumnsList, sapFromColsDataTypes, jobId);
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"), "Inserted " + insertCount
										+ " Records in " + ((jobEndTime - jobStartTime) / 1000) + " Sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					// db
					JSONArray sourceOperatorList = getConnectedFromOperatorIds(request,
							String.valueOf(fromOperator.get("operatorId")), flowchartData);
					Object sourceOperatorId = (Object) sourceOperatorList.get(0);
					JSONObject sourceOperator = (JSONObject) operators.get(String.valueOf(sourceOperatorId));
					JSONObject sourceConnObj = (JSONObject) sourceOperator.get("connObj");
					if (sourceConnObj != null && !sourceConnObj.isEmpty()) {
						sourceConnection = componentUtilities.getConnection(sourceConnObj);
					}
					componentUtilities.dropStagingTable(toTableName, (Connection) toConnection);
					query = "SELECT * FROM " + String.valueOf(sourceOperator.get("tableName")).replaceAll(":", ".")
							+ " WHERE 1=2";
					JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(query,
							(Connection) sourceConnection, sourceConnObj);
					List dataTypesList = (List) columnsObject.get("dataTypesList");
					List columnsList = (List) columnsObject.get("columnsList");
					componentUtilities.createTable(toTableName, columnsList, dataTypesList, (Connection) toConnection);
					String insertQuery = generateInsertQuery(toTableName, columnsList);
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							columnsList, fromDataList, jobId, infoObject);
					insertCount = (int) infoObject.get("insertCount");
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								insertCount + " Records Rejected ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

				}
			} else {
				// files

				String iconType = String.valueOf(tooperator.get("iconType"));
				String orginalName = (String) tooperator.get("userFileName");
				String filename = "";
				String message = "";
				String userName = (String) request.getSession(false).getAttribute("ssUsername");
				String filePath = etlFilePath + "ETL_EXPORT_" + File.separator + userName;

				String fileExt = String.valueOf(tooperator.get("iconType")).toLowerCase();
				String fileName = "V10ETLExport_" + tooperator.get("timeStamp") + "." + fileExt;
				filename = fileName;
				if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& ("XLSX".equalsIgnoreCase(iconType) || "XLS".equalsIgnoreCase(iconType))) {
					insertCount = fileProcessJobService.insertIntoXLSXFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& ("CSV".equalsIgnoreCase(iconType) || "TXT".equalsIgnoreCase(iconType))) {
					insertCount = fileProcessJobService.insertIntoTextOrCSVFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType) && "XML".equalsIgnoreCase(iconType)) {
					insertCount = fileProcessJobService.insertIntoXMLFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& "JSON".equalsIgnoreCase(iconType)) {
					insertCount = fileProcessJobService.insertIntoJSONFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				}

				long jobEndTime = System.currentTimeMillis();
				long jobStartTime = (long) request.getAttribute("jobStartTime");

				message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + filename + "','"
						+ orginalName + "') >Click here to download the " + iconType + " file</a>.";//

				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							insertCount + " Records Rejected ", "INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}

				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), message, "INFO", 20, "Y",
							jobId);
				} catch (Exception ex) {
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {

				if (sourceConnection != null) {
					((Connection) sourceConnection).close();
				}
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processRejectedComponentJob1(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {
			List fromDataList = new ArrayList();
			String query = "";
			List toColumnsList = new ArrayList();
			JSONObject operator = (JSONObject) operators.get(operatorId);
//            JSONArray fromOperatorList = (JSONArray) operator.get("connectedFrom");
			JSONArray actualFromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object actualFromOperatorId = (Object) actualFromOperatorList.get(0);
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request, String.valueOf(actualFromOperatorId),
					flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			String fromIconType = (String) fromOperator.get("iconType");
			if (fromIconType != null && "FILTER".equalsIgnoreCase(fromIconType)) {
				JSONObject rejectRecordsObj = filterRejectedRecords(request, fromOperator, operators, flowchartData,
						jobId);
				fromDataList = (List) rejectRecordsObj.get("fromDataList");
				query = (String) rejectRecordsObj.get("query");
				toColumnsList = (List) fromOperator.get("simpleColumnsList");
			} else if (fromIconType != null && "UNIQUE".equalsIgnoreCase(fromIconType)) {
				JSONObject rejectRecordsObj = uniqueRejectedRecords(request, fromOperator, operators, flowchartData,
						jobId);
				fromDataList = (List) rejectRecordsObj.get("fromDataList");
				query = (String) rejectRecordsObj.get("query");
				toColumnsList = (List) fromOperator.get("simpleColumnsList");
			} else if (fromIconType != null && "MERGE".equalsIgnoreCase(fromIconType)) {
				JSONObject rejectRecordsObj = mergeRejectedRecords(request, fromOperator, operators, flowchartData,
						jobId);
				fromDataList = (List) rejectRecordsObj.get("fromDataList");
				query = (String) rejectRecordsObj.get("query");
				toColumnsList = (List) fromOperator.get("simpleColumnsList");
			}

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

			String iconType = "XLSX";
			String orginalName = (String) tooperator.get("userFileName");
			String filename = "";
			String message = "";
			String userName = (String) request.getSession(false).getAttribute("ssUsername");
			String filePath = etlFilePath + "ETL_EXPORT_" + File.separator + userName;

			String fileExt = iconType.toLowerCase();
			String fileName = "V10ETLExport_" + tooperator.get("timeStamp") + "." + fileExt;
			filename = fileName;
			insertCount = fileProcessJobService.insertIntoXLSXFile(request, tooperator, toColumnsList, fromDataList,
					filePath, fileName);

			long jobEndTime = System.currentTimeMillis();
			long jobStartTime = (long) request.getAttribute("jobStartTime");

			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), insertCount + " Records Rejected ",
						"INFO", 20, "Y", jobId);
			} catch (Exception ex) {
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processMergeComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);
			String tableName = (String) operator.get("tableName");
//            JSONArray connectedTo = (JSONArray) operator.get("connectedTo");
			JSONArray connectedTo = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			String toOperatorId = String.valueOf(connectedTo.get(0));
			JSONObject toOperator = (JSONObject) operators.get(toOperatorId);
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) operator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToTargetDBTable(request, fromOperator, toConnObj, jobId);
				} else {
					// db
					if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
							&& fromConnObj.containsKey("HOST_NAME")
							&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
							&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
							&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
							&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
							&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
						fromOperator = otherDBToTargetDBTable(request, fromOperator, toConnObj, jobId);
					}
				}
			} else {
				// files
				fromOperator = fileToTargetDBTable(request, fromOperator, toConnObj, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}

			// JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
			JSONObject trfmRules = (JSONObject) operator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			List<Object[]> columnsList = new ArrayList();
			List<Object[]> pkColumnsList = new ArrayList();
			List<Object[]> updateColsList = new ArrayList();

//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();
			String fromTable = (String) fromOperator.get("tableName");
			String OriginalTableName = (String) fromOperator.get("tableName");

			String skipRejectedRecords = "";
			if (trfmRules != null && !trfmRules.isEmpty()) {

				skipRejectedRecords = (String) trfmRules.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRules.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);

					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn.contains("/")) {
						if (destinationColumn.contains(":")) {
							destinationColumn = destinationColumn.split(":")[0] + ":" + "\""
									+ destinationColumn.split(":")[1] + "\"";
						} else {
							destinationColumn = "\"" + destinationColumn.split(":")[1] + "\"";
						}
					}

					if (destinationColumn != null && !"null".equalsIgnoreCase(destinationColumn)
							&& destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)
							&& !"null".equalsIgnoreCase(sourceColumn)) {

						if (sourceColumn.contains("/")) {
							if (sourceColumn.contains(":")) {
								sourceColumn = sourceColumn.split(":")[0] + ":" + "\"" + sourceColumn.split(":")[1]
										+ "\"";
							} else {
								sourceColumn = "\"" + sourceColumn.split(":")[1] + "\"";
							}
						}

						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)
							&& !"null".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
					Object[] colsobj = { sourceColumn, destinationColumn };
					columnsList.add(colsobj);

					String primaryKey = (String) rowData.get("primaryKey");
					if ("Y".equalsIgnoreCase(primaryKey)) {
						Object[] pkData = { sourceColumn, destinationColumn };
						pkColumnsList.add(pkData);
					}

					String updateKey = (String) rowData.get("updateKey");
					if ("Y".equalsIgnoreCase(updateKey)) {
						Object[] updateData = { sourceColumn, destinationColumn };
						updateColsList.add(updateData);
					}

				}
			}

			// new Method
			// String fromTable = (String) fromOperator.get("tableName");
			// JSONArray primaryKeys = (JSONArray) trfmRules.get("uniqueKeys");
			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
//			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
//			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
//					fromConnObj);
//			List dataTypesList = (List) columnsObject.get("dataTypesList");
//			List createTableColumnsList = (List) columnsObject.get("columnsList");
//			fromColumnsList = columnsList;
//			toColumnsList = columnsList;
//            componentUtilities.createTable(toTableName, createTableColumnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					String operatorType = (String) trfmRules.get("operatorType");

					if (operatorType != null && "Insert Or Update".equalsIgnoreCase(operatorType)) {
						// updateColsList = (JSONArray) trfmRules.get("updateColsList");

						JSONObject getUpdatedCols = processJobComponentsDAO.insertOrMergeCount(request, toTableName,
								columnsList, updateColsList, fromConnection, fromTable, pkColumnsList, jobId);

						String ssUserName = request.getSession(false).getAttribute("ssUsername").toString();
						String orgnId = request.getSession(false).getAttribute("ssOrgId").toString();
						String targetTable = toTableName;
						String sourceTable = fromOperator.get("originalTableName") != null
								? (String) fromOperator.get("originalTableName")
								: (String) fromOperator.get("tableName");
						String targetTableCount = "";
						String rejectedRecords = "";
						String comments = "";
						String subJobId = operator.get("subJobId").toString();
						String sourceCount = "";
						String proccessedRecordCount = "";
						String rejectedRecordCount = "";

						insertCount = processJobComponentsDAO.mergeInsert(request, toTableName, columnsList,
								updateColsList, fromConnection, fromTable, pkColumnsList, jobId);

						int updateCount = processJobComponentsDAO.mergeUpdate(request, toTableName, columnsList,
								updateColsList, fromConnection, fromTable, pkColumnsList, jobId);

						try {
							componentUtilities.processETLReconciliationUpdateTarget(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
									String.valueOf(insertCount), subJobId, String.valueOf(columnsList.size()));
						} catch (Exception ex) {
						}

						try {
							int insertUpdateColsCount = componentUtilities.updateETLReconciliationClob(orgnId, jobId,
									ssUserName, sourceTable, sourceCount, targetTable, String.valueOf(insertCount),
									rejectedRecordCount, comments, subJobId, getUpdatedCols);
						} catch (Exception ex) {
						}

//						insertCount = processJobComponentsDAO.mergeInsertOrUpdate(request, toTableName, columnsList,
//								new ArrayList(updateColsList), fromConnection, fromTable, new ArrayList(primaryKeys),
//								jobId);
						long jobEndTime = System.currentTimeMillis();
						long jobStartTime = (long) request.getAttribute("jobStartTime");
//                        try {
//                            componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
//                                    (String) request.getSession(false).getAttribute("ssOrgId"),
//                                    "Merged " + insertCount + " Records in " + (jobEndTime - jobStartTime) / 1000 + " sec", "INFO", 20, "Y", jobId);
//                        } catch (Exception ex) {
//                        }
						try {
							componentUtilities
									.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											"Inserted " + insertCount + " Records  in "
													+ (jobEndTime - jobStartTime) / 1000 + " sec",
											"INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}

						int updateColsCount = getUpdatedCols.entrySet().stream()
								.mapToInt(entry -> (int) ((Map.Entry) entry).getValue()).sum();

						try {
							componentUtilities
									.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											"Updated " + updateColsCount + " Values  in "
													+ (jobEndTime - jobStartTime) / 1000 + " sec",
											"INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}

					} else if (operatorType != null && "Insert".equalsIgnoreCase(operatorType)) {
						String subJobId = operator.get("subJobId").toString();
						insertCount = processJobComponentsDAO.mergeInsert(request, toTableName, columnsList,
								new ArrayList(updateColsList), fromConnection, fromTable, pkColumnsList, jobId);

						try {
							componentUtilities.processETLReconciliationUpdateTarget(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"), jobId, toTableName,
									String.valueOf(insertCount), subJobId, String.valueOf(columnsList.size()));
						} catch (Exception ex) {
						}

						long jobEndTime = System.currentTimeMillis();
						long jobStartTime = (long) request.getAttribute("jobStartTime");
						try {
							componentUtilities
									.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											"Inserted " + insertCount + " Records in "
													+ (jobEndTime - jobStartTime) / 1000 + " sec",
											"INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}
					} else if (operatorType != null && "Update".equalsIgnoreCase(operatorType)) {
						// updateColsList = (JSONArray) trfmRules.get("updateColsList");

//						insertCount = processJobComponentsDAO.mergeUpdate(request, toTableName, columnsList,
//								new ArrayList(updateColsList), fromConnection, fromTable, new ArrayList(primaryKeys),
//								jobId);
						JSONObject getUpdatedCols = processJobComponentsDAO.insertOrMergeCount(request, toTableName,
								columnsList, updateColsList, fromConnection, fromTable, pkColumnsList, jobId);

						int updateCount = getUpdatedCols.entrySet().stream()
								.mapToInt(entry -> (int) ((Map.Entry) entry).getValue()).sum();
						int updateCount1 = processJobComponentsDAO.mergeUpdate(request, toTableName, columnsList,
								updateColsList, fromConnection, fromTable, pkColumnsList, jobId);

						long jobEndTime = System.currentTimeMillis();
						long jobStartTime = (long) request.getAttribute("jobStartTime");
						try {
							componentUtilities.processETLLog(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"), "Updated " + updateCount
											+ " Values in " + (jobEndTime - jobStartTime) / 1000 + " sec",
									"INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}

						String ssUserName = request.getSession(false).getAttribute("ssUsername").toString();
						String orgnId = request.getSession(false).getAttribute("ssOrgId").toString();
						String targetTable = toTableName;
						String sourceTable = fromOperator.get("tableName").toString();
						String targetTableCount = "";
						String rejectedRecords = "";
						String comments = "";
						String subJobId = operator.get("subJobId").toString();
						String sourceCount = "";
						String proccessedRecordCount = "";
						String rejectedRecordCount = "";

						try {
							int insertUpdateColsCount = componentUtilities.updateETLReconciliationClob(orgnId, jobId,
									ssUserName, sourceTable, sourceCount, targetTable, proccessedRecordCount,
									rejectedRecordCount, comments, subJobId, getUpdatedCols);
						} catch (Exception ex) {
						}

					}

				}
//				else {
//					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
//					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);
//
//					long startInsertTime = System.currentTimeMillis();
//					JSONObject infoObject = new JSONObject();
//					int count = 0;
//					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
//					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
//							toColumnsList, fromDataList, jobId, infoObject);
//					count = (int) infoObject.get("insertCount");
//					int deleteCount = processJobComponentsDAO.deleteDuplicates(request, toTableName,
//							pkColumnsList, toConnection, jobId);
//					insertCount = count - deleteCount;
//					System.out.println(
//							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
//					long jobEndTime = System.currentTimeMillis();
//					long jobStartTime = (long) request.getAttribute("jobStartTime");
//					try {
//						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
//								(String) request.getSession(false).getAttribute("ssOrgId"),
//								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
//					} catch (Exception ex) {
//					}
//				}
			}

			try {
				componentUtilities.processETLJobPreview((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId,
						String.valueOf(toOperator.get("operatorId")), String.valueOf(insertCount));
			} catch (Exception ex) {
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				Object toConnectionStagingTable = request.getAttribute("toConnStagingTable");
				if (toConnectionStagingTable != null) {
					componentUtilities.dropStagingTable(String.valueOf(toConnectionStagingTable), toConnection);
				}
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processSCD6ComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);
			String tableName = (String) operator.get("tableName");
//            JSONArray connectedTo = (JSONArray) operator.get("connectedTo");
			JSONArray connectedTo = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			String toOperatorId = String.valueOf(connectedTo.get(0));
			JSONObject toOperator = (JSONObject) operators.get(toOperatorId);
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}
			JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
			JSONArray historyCols = (JSONArray) trfmRules.get("historyCols");
			boolean tableAltered2 = alterTableSCDType2(new HashMap(toOperator), toConnection);
			boolean tableAltered3 = alterTableSCDType3(new HashMap(toOperator), toConnection, historyCols);
//            JSONArray fromOperatorList = (JSONArray) operator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToTargetDBTable(request, fromOperator, toConnObj, jobId);
				} else {
					// db
					if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
							&& fromConnObj.containsKey("HOST_NAME")
							&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
							&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
							&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
							&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
							&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
						fromOperator = otherDBToTargetDBTable(request, fromOperator, toConnObj, jobId);
					}
				}
			} else {
				// files
				fromOperator = fileToTargetDBTable(request, fromOperator, toConnObj, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}

			String statusLabel = (String) toOperator.get("statusLabel");
			JSONArray primaryKeys = componentUtilities.getPrimaryKeyColumns(toConnection, statusLabel);

			// new Method
			List fromColumnsList = null;
			List toColumnsList = null;

			String fromTable = (String) fromOperator.get("tableName");

			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = new ArrayList(columnsList);
			toColumnsList = new ArrayList(columnsList);
			toColumnsList.add("ADDRESS_KEY");
			toColumnsList.add("START_DATE");
			toColumnsList.add("END_DATE");
			toColumnsList.add("FLAG");
//            componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					String operatorType = (String) trfmRules.get("operatorType");

					List updateColsList = new ArrayList(fromColumnsList);
					JSONObject countObject = processJobComponentsDAO.mergeInsertOrUpdateSCDType6(request, toTableName,
							columnsList, updateColsList, fromConnection, fromTable, new ArrayList(primaryKeys),
							new ArrayList(historyCols), jobId);
					insertCount = (int) countObject.get("insertCount");
					int updateCount = (int) countObject.get("updateCount");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");

					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Updated " + updateCount + " Records in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"), "Inserted " + insertCount
										+ " Records in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					int count = (int) infoObject.get("insertCount");
					int deleteCount = processJobComponentsDAO.deleteDuplicates(request, toTableName,
							new ArrayList(primaryKeys), toConnection, jobId);
					insertCount = count - deleteCount;
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				Object toConnectionStagingTable = request.getAttribute("toConnStagingTable");
				if (toConnectionStagingTable != null) {
					componentUtilities.dropStagingTable(String.valueOf(toConnectionStagingTable), toConnection);
				}
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processSCD4ComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);
			String tableName = (String) operator.get("tableName");
//            JSONArray connectedTo = (JSONArray) operator.get("connectedTo");
			JSONArray connectedTo = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			String toOperatorId = String.valueOf(connectedTo.get(0));
			JSONObject toOperator = (JSONObject) operators.get(toOperatorId);
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}
			JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
			JSONArray historyCols = (JSONArray) trfmRules.get("historyCols");
//            boolean tableAltered = alterTableSCDType3(new HashMap(toOperator), toConnection, historyCols);
//            JSONArray fromOperatorList = (JSONArray) operator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToTargetDBTable(request, fromOperator, toConnObj, jobId);
				} else {
					// db
					if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
							&& fromConnObj.containsKey("HOST_NAME")
							&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
							&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
							&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
							&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
							&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
						fromOperator = otherDBToTargetDBTable(request, fromOperator, toConnObj, jobId);
					}
				}
			} else {
				// files
				fromOperator = fileToTargetDBTable(request, fromOperator, toConnObj, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}

			String statusLabel = (String) toOperator.get("statusLabel");
			JSONArray primaryKeys = componentUtilities.getPrimaryKeyColumns(toConnection, statusLabel);

			// new Method
			List fromColumnsList = null;
			List toColumnsList = null;

			String fromTable = (String) fromOperator.get("tableName");

			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = new ArrayList(columnsList);
			toColumnsList = new ArrayList(columnsList);

			List historyTableCols = new ArrayList(columnsList);
			historyTableCols.add("START_DATE");
			historyTableCols.add("END_DATE");
			historyTableCols.add("FLAG");

			List historyTableDataTypes = new ArrayList(dataTypesList);
			historyTableDataTypes.add("DATE");
			historyTableDataTypes.add("DATE");
			historyTableDataTypes.add("VARCHAR2(2)");
			boolean historyTableCreated = componentUtilities.createTable(toTableName + "_HISTORY", historyTableCols,
					historyTableDataTypes, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					String operatorType = (String) trfmRules.get("operatorType");

					List updateColsList = new ArrayList(fromColumnsList);
					int mergeCount = processJobComponentsDAO.mergeInsertOrUpdate(request, toTableName, columnsList,
							updateColsList, fromConnection, fromTable, new ArrayList(primaryKeys), jobId);

					JSONObject countObject = processJobComponentsDAO.mergeSCDType4HistoryTable(request,
							toTableName + "_HISTORY", columnsList, updateColsList, fromConnection, fromTable,
							new ArrayList(primaryKeys), jobId);
					insertCount = (int) countObject.get("insertCount");
					int updateCount = (int) countObject.get("updateCount");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Merged " + mergeCount + " Records in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
//                    try {
//                        componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
//                                (String) request.getSession(false).getAttribute("ssOrgId"),
//                                "Updated " + updateCount + " Records in History Table in " + (jobEndTime - jobStartTime) / 1000 + " sec", "INFO", 20, "Y", jobId);
//                    } catch (Exception ex) {
//                    }

					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"), "Inserted " + insertCount
										+ " Records in History Table in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					int count = (int) infoObject.get("insertCount");
					int deleteCount = processJobComponentsDAO.deleteDuplicates(request, toTableName,
							new ArrayList(primaryKeys), toConnection, jobId);
					insertCount = count - deleteCount;
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				Object toConnectionStagingTable = request.getAttribute("toConnStagingTable");
				if (toConnectionStagingTable != null) {
					componentUtilities.dropStagingTable(String.valueOf(toConnectionStagingTable), toConnection);
				}
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processSCD3ComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);
			String tableName = (String) operator.get("tableName");
//            JSONArray connectedTo = (JSONArray) operator.get("connectedTo");
			JSONArray connectedTo = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			String toOperatorId = String.valueOf(connectedTo.get(0));
			JSONObject toOperator = (JSONObject) operators.get(toOperatorId);
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}
			JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
			JSONArray historyCols = (JSONArray) trfmRules.get("historyCols");
			boolean tableAltered = alterTableSCDType3(new HashMap(toOperator), toConnection, historyCols);
//            JSONArray fromOperatorList = (JSONArray) operator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToTargetDBTable(request, fromOperator, toConnObj, jobId);
				} else {
					// db
					if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
							&& fromConnObj.containsKey("HOST_NAME")
							&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
							&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
							&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
							&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
							&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
						fromOperator = otherDBToTargetDBTable(request, fromOperator, toConnObj, jobId);
					}
				}
			} else {
				// files
				fromOperator = fileToTargetDBTable(request, fromOperator, toConnObj, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}

			String statusLabel = (String) toOperator.get("statusLabel");
			JSONArray primaryKeys = componentUtilities.getPrimaryKeyColumns(toConnection, statusLabel);

			// new Method
			List fromColumnsList = null;
			List toColumnsList = null;

			String fromTable = (String) fromOperator.get("tableName");

			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = new ArrayList(columnsList);
			toColumnsList = new ArrayList(columnsList);

//            componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);
			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					String operatorType = (String) trfmRules.get("operatorType");

					List updateColsList = new ArrayList(fromColumnsList);
					JSONObject countObject = processJobComponentsDAO.mergeInsertOrUpdateSCDType3(request, toTableName,
							columnsList, updateColsList, fromConnection, fromTable, new ArrayList(primaryKeys),
							new ArrayList(historyCols), jobId);
					insertCount = (int) countObject.get("insertCount");
					int updateCount = (int) countObject.get("updateCount");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");

					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Updated " + updateCount + " Records in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"), "Inserted " + insertCount
										+ " Records in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					int count = (int) infoObject.get("insertCount");
					int deleteCount = processJobComponentsDAO.deleteDuplicates(request, toTableName,
							new ArrayList(primaryKeys), toConnection, jobId);
					insertCount = count - deleteCount;
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				Object toConnectionStagingTable = request.getAttribute("toConnStagingTable");
				if (toConnectionStagingTable != null) {
					componentUtilities.dropStagingTable(String.valueOf(toConnectionStagingTable), toConnection);
				}
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processSCD2ComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);
			String tableName = (String) operator.get("tableName");
//            JSONArray connectedTo = (JSONArray) operator.get("connectedTo");
			JSONArray connectedTo = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			String toOperatorId = String.valueOf(connectedTo.get(0));
			JSONObject toOperator = (JSONObject) operators.get(toOperatorId);
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}
			JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
			boolean tableAltered = alterTableSCDType2(new HashMap(toOperator), toConnection);
//            JSONArray fromOperatorList = (JSONArray) operator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToTargetDBTable(request, fromOperator, toConnObj, jobId);
				} else {
					// db
					if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
							&& fromConnObj.containsKey("HOST_NAME")
							&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
							&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
							&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
							&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
							&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
						fromOperator = otherDBToTargetDBTable(request, fromOperator, toConnObj, jobId);
					}
				}
			} else {
				// files
				fromOperator = fileToTargetDBTable(request, fromOperator, toConnObj, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}

			String statusLabel = (String) toOperator.get("statusLabel");
			JSONArray primaryKeys = componentUtilities.getPrimaryKeyColumns(toConnection, statusLabel);

			// new Method
			List fromColumnsList = null;
			List toColumnsList = null;

			String fromTable = (String) fromOperator.get("tableName");

			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = new ArrayList(columnsList);
			toColumnsList = new ArrayList(columnsList);
			toColumnsList.add("ADDRESS_KEY");
			toColumnsList.add("START_DATE");
			toColumnsList.add("END_DATE");
			toColumnsList.add("FLAG");
//            componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					String operatorType = (String) trfmRules.get("operatorType");

					List updateColsList = new ArrayList(fromColumnsList);
					JSONObject countObject = processJobComponentsDAO.mergeInsertOrUpdateSCDType2(request, toTableName,
							columnsList, updateColsList, fromConnection, fromTable, new ArrayList(primaryKeys), jobId);
					insertCount = (int) countObject.get("insertCount");
					int updateCount = (int) countObject.get("updateCount");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");

					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Updated " + updateCount + " Records in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"), "Inserted " + insertCount
										+ " Records in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					int count = (int) infoObject.get("insertCount");
					int deleteCount = processJobComponentsDAO.deleteDuplicates(request, toTableName,
							new ArrayList(primaryKeys), toConnection, jobId);
					insertCount = count - deleteCount;
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				Object toConnectionStagingTable = request.getAttribute("toConnStagingTable");
				if (toConnectionStagingTable != null) {
					componentUtilities.dropStagingTable(String.valueOf(toConnectionStagingTable), toConnection);
				}
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processSCD1ComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject operator = (JSONObject) operators.get(operatorId);
			String tableName = (String) operator.get("tableName");
//            JSONArray connectedTo = (JSONArray) operator.get("connectedTo");
			JSONArray connectedTo = getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")),
					flowchartData);
			String toOperatorId = String.valueOf(connectedTo.get(0));
			JSONObject toOperator = (JSONObject) operators.get(toOperatorId);
			JSONObject toConnObj = (JSONObject) toOperator.get("connObj");
			String toTableName = (String) toOperator.get("tableName");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) operator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(operator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToTargetDBTable(request, fromOperator, toConnObj, jobId);
				} else {
					// db
					if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
							&& fromConnObj.containsKey("HOST_NAME")
							&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
							&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
							&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
							&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
							&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
						fromOperator = otherDBToTargetDBTable(request, fromOperator, toConnObj, jobId);
					}
				}
			} else {
				// files
				fromOperator = fileToTargetDBTable(request, fromOperator, toConnObj, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRules = (JSONObject) operator.get("trfmRules");

			String statusLabel = (String) toOperator.get("statusLabel");
			JSONArray primaryKeys = componentUtilities.getPrimaryKeyColumns(toConnection, statusLabel);

			// new Method
			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");

			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;
//            componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					String operatorType = (String) trfmRules.get("operatorType");

					List updateColsList = new ArrayList(fromColumnsList);
					insertCount = processJobComponentsDAO.mergeInsertOrUpdate(request, toTableName, columnsList,
							updateColsList, fromConnection, fromTable, new ArrayList(primaryKeys), jobId);

					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");

					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"), " " + insertCount
										+ " Records Inserted/Updated in " + (jobEndTime - jobStartTime) / 1000 + " sec",
								"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}

				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					int count = (int) infoObject.get("insertCount");
					int deleteCount = processJobComponentsDAO.deleteDuplicates(request, toTableName,
							new ArrayList(primaryKeys), toConnection, jobId);
					insertCount = count - deleteCount;
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				Object toConnectionStagingTable = request.getAttribute("toConnStagingTable");
				if (toConnectionStagingTable != null) {
					componentUtilities.dropStagingTable(String.valueOf(toConnectionStagingTable), toConnection);
				}
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processUniqueComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");

			JSONArray uniqueKeys = (JSONArray) trfmRules.get("uniqueKeys");
			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
			String colObjQuery = "SELECT * FROM " + fromTable + " WHERE 1=2 ";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjQuery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;
			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					int count = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsList,
							toColumnsList, toConnection, query, jobId);
					int deleteCount = processJobComponentsDAO.deleteDuplicates(request, toTableName, uniqueKeys,
							toConnection, jobId);
					insertCount = count - deleteCount;
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					int count = (int) infoObject.get("insertCount");
					int deleteCount = processJobComponentsDAO.deleteDuplicates(request, toTableName, uniqueKeys,
							toConnection, jobId);
					insertCount = count - deleteCount;
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processFilterComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");
			String originalTableName = fromOperator.get("originalTableName") != null
					? (String) fromOperator.get("originalTableName")
					: null;

			String whereClauseCond = "";
			JSONArray whereClauseData = (JSONArray) trfmRules.get("whereClauseData");
			for (int i = 0; i < whereClauseData.size(); i++) {
				String mappedDataStr = (String) whereClauseData.get(i);
				JSONObject mappedData = (JSONObject) JSONValue.parse(mappedDataStr);
				int index = 0;
				for (Object key : mappedData.keySet()) {
					index++;
					JSONObject filterInfo = (JSONObject) mappedData.get(key);
					String columnName = (String) filterInfo.get("columnNameActualValue");

					if (columnName.contains(":")) {
						columnName = columnName.split(":")[1];
					}
					if (columnName.contains(".")) {
						columnName = columnName.split("\\.")[1];
					}

					String operator = (String) filterInfo.get("operator");
					String staticValue = (String) filterInfo.get("staticValue");
					String andOrOperator = (String) filterInfo.get("andOrOperator");
					if (index == mappedData.size()) {
						whereClauseCond += columnName + " " + operator + " '" + staticValue + "' ";
					} else {
						if ("IN".equalsIgnoreCase(operator) || "NOT IN".equalsIgnoreCase(operator)) {
							if (staticValue.contains("##")) {
								staticValue = "('" + staticValue.replaceAll("#{2,}", "','") + "')";
							} else {
								staticValue = "('" + staticValue + "')";
							}
							whereClauseCond += columnName + " " + operator + " " + staticValue + " " + andOrOperator
									+ " ";
						} else if ("LIKE".equalsIgnoreCase(operator)) {
							whereClauseCond += columnName + " " + operator + " '%" + staticValue + "%' " + andOrOperator
									+ " ";
						} else {
							whereClauseCond += columnName + " " + operator + " '" + staticValue + "' " + andOrOperator
									+ " ";
						}

					}
				}
			}

			String query = "SELECT * FROM " + fromTable + " WHERE " + whereClauseCond;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;
			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsList,
							toColumnsList, toConnection, query, jobId);

					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					insertCount = (int) infoObject.get("insertCount");
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processRowsRangeComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");
			String originalTableName = fromOperator.get("originalTableName") != null
					? (String) fromOperator.get("originalTableName")
					: null;

			String rangeCondition = "";
			JSONObject rowsRangeObj = (JSONObject) trfmRules.get("rowsRangeObj");
			if (rowsRangeObj != null && !rowsRangeObj.isEmpty()) {

				if (rowsRangeObj.get("startIndex") != null && rowsRangeObj.get("limit") != null) {
					long startIndex = (long) rowsRangeObj.get("startIndex");
					long limit = (long) rowsRangeObj.get("limit");
					rangeCondition = "  OFFSET " + startIndex + " ROWS FETCH NEXT " + limit + " ROWS ONLY ";
				}
			}
			String query = "SELECT * FROM " + fromTable + rangeCondition;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;

			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsList,
							toColumnsList, toConnection, query, jobId);

					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					insertCount = (int) infoObject.get("insertCount");
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processSortComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");
			String originalTableName = fromOperator.get("originalTableName") != null
					? (String) fromOperator.get("originalTableName")
					: null;

			String orderByClause = "";
			JSONArray orderByObj = (JSONArray) trfmRules.get("orderByData");
			if (orderByObj != null && !orderByObj.isEmpty()) {
				int orderIndex = 0;
				for (int i = 0; i < orderByObj.size(); i++) {

					JSONObject orderObj = (JSONObject) orderByObj.get(i);
					if (orderObj != null && !orderObj.isEmpty() && orderObj.get("columnName") != null
							&& !"".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))
							&& !"null".equalsIgnoreCase(String.valueOf(orderObj.get("columnName")))) {
//                        String orderByColName = (String) orderObj.get("columnName");
						String orderByColName = (String) orderObj.get("columnNameActualValue");

						orderByColName = orderByColName.replaceAll(fromTable + ":", fromTable + ".");
						orderByClause += "" + orderByColName + " " + " "
								+ (("DESC".equalsIgnoreCase(String.valueOf(orderObj.get("order")))) ? "DESC" : "ASC");
						if (orderIndex != orderByObj.size() - 1) {
							orderByClause += ",";
						}
						orderIndex++;
					}
				}
				// columnName,direction
				if (orderByClause != null && !"".equalsIgnoreCase(orderByClause)
						&& !"null".equalsIgnoreCase(orderByClause)) {
					orderByClause = " ORDER BY " + orderByClause + " ";
				}
			}
			String query = "SELECT * FROM " + fromTable + orderByClause;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;

			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsList,
							toColumnsList, toConnection, query, jobId);

					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					insertCount = (int) infoObject.get("insertCount");
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processGroupByComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRulesDataMap = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();
			JSONArray aliasColumnsList = new JSONArray();

			String fromTable = (String) fromOperator.get("tableName");
			String originalTableName = fromOperator.get("originalTableName") != null
					? (String) fromOperator.get("originalTableName")
					: null;

			String groupByQuery = "";
			JSONArray groupByData = (JSONArray) trfmRulesDataMap.get("groupByData");
			for (int i = 0; i < groupByData.size(); i++) {
				JSONObject groupByDataObj = (JSONObject) groupByData.get(i);
//                String groupByColName = (String) groupByDataObj.get("columnName");
				String groupByColName = (String) groupByDataObj.get("columnNameActualValue");
				if (originalTableName != null && !"".equalsIgnoreCase(originalTableName)) {
					groupByColName = groupByColName.replaceAll(originalTableName + ":", fromTable + ".");
					fromColumnsList.add(groupByColName.replaceAll(":", "."));
					if (groupByColName.contains(":")) {
						toColumnsList.add(groupByColName.split(":")[1]);
					} else {
						toColumnsList.add(groupByColName.split(":")[1]);
					}

				}
				groupByColName = groupByColName.replaceAll(fromTable + ":", fromTable + ".");

				if (i < (groupByData.size() - 1)) {
					groupByQuery += groupByColName + ", ";
				} else {
					groupByQuery += groupByColName + " ";
				}

			}
			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)
							&& !"null".equalsIgnoreCase(sourceColumn)) {
						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)
							&& !"null".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)
							&& !"null".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)
							&& !"null".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);

						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
				}
			}
			List fromColumnsAliasList = new ArrayList();
//            fromColumnsList = new ArrayList(fromColumnsSet);
//            toColumnsList = new ArrayList(toColumnsSet);

			if (groupByQuery != null && !"".equalsIgnoreCase(groupByQuery) && !"null".equalsIgnoreCase(groupByQuery)) {
				groupByQuery = " GROUP BY  " + groupByQuery.replace(":", ".") + " ";
			}

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
				if (i < (fromColumnsList.size() - 1)) {
					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}

			String query = "SELECT " + columnsListStr + " FROM " + fromTable + groupByQuery;
			System.out.println("query :: " + query);
			String selectQuery = "SELECT * FROM (" + query + ") WHERE 1=2";

			System.out.println("query :: " + query);

			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(selectQuery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& fromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))
					|| "Y".equalsIgnoreCase(String.valueOf(skipRejectedRecords))) {
				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))
						&& !"Y".equalsIgnoreCase(String.valueOf(skipRejectedRecords))) {

					insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsAliasList,
							toColumnsList, toConnection, query, jobId);

					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					insertCount = (int) infoObject.get("insertCount");
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {

				if (fromConnection != null) {
					fromConnection.close();
				}
				if (toConnection != null) {
					toConnection.close();
				}

			} catch (Exception e) {
			}
		}
		return operators;
	}

	public Map processQueryComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {
		Connection fromConnection = null;
		Object toConnection = null;
		Connection currentConnection = null;
		PreparedStatement preparedStatement = null;
		CallableStatement callableStmt = null;
		JSONObject resultObj = new JSONObject();
		int insertCount = 0;
		int count = 0;
		try {
			List fromColumnsList = new ArrayList();
			List fromAliasColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
			List fromDataList = new ArrayList();

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

			JSONObject selectedOperator = (JSONObject) operators.get(operatorId);
			JSONObject fromConnObj = (JSONObject) selectedOperator.get("queryConnObj");

			JSONObject trfmRules = (JSONObject) selectedOperator.get("trfmRules");
			String query = (String) trfmRules.get("queryData");
			query = query.toUpperCase();

//            int toIndex = query.indexOf("FROM");
//            String columnsStr = query.substring(6, toIndex);
//            String restOfQuery = query.substring(toIndex);
//            String columnsListStr = "";
//
//            Pattern pattern = Pattern.compile("[^ _.a-zA-Z0-9]", Pattern.CASE_INSENSITIVE);
//            String[] colsArray = columnsStr.split(",");
//            for (int i = 0; i < colsArray.length; i++) {
//                String column = colsArray[i];
//                String fromColumn = column;
//                if (column != null && column.contains(" AS ")) {
//                    String fromColumn0 = column.split(" AS ")[0];
//                    String fromColumn1 = column.split(" AS ")[1];
//                    fromColumn0 = fromColumn0.trim();
//                    if ("''".equalsIgnoreCase(fromColumn0)) {
//                        fromColumn0 = "'                                        '";
//                    }
//                    fromColumnsList.add(fromColumn0 + " AS " + fromColumn1);
//                } else if (pattern.matcher(column).find()) {
//                    if ("''".equalsIgnoreCase(column)) {
//                        column = "'                                        ' AS " + " COLUMN_" + i;
//                    } else {
//                        column = column + " AS " + " COLUMN_" + i;
//                    }
//
//                    fromColumnsList.add(column);
//                } else {
//                    fromColumnsList.add(column);
//                }
//
//                toColumnsList.add(column);
//            }
//
//            for (int i = 0; i < fromColumnsList.size(); i++) {
//                if (i < (fromColumnsList.size() - 1)) {
//                    columnsListStr += fromColumnsList.get(i) + ", ";
//                } else {
//                    columnsListStr += fromColumnsList.get(i);
//                }
//
//            }
//            fromAliasColumnsList.addAll(toColumnsList);
//
//            query = "SELECT " + columnsListStr + " " + restOfQuery;
			if (query.startsWith("SELECT")) {

				String selectQuery = "SELECT * FROM (" + query + ") WHERE 1=2";
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
				JSONObject columnsObj = componentUtilities.getColumnsObjFromQuery(selectQuery, fromConnection,
						fromConnObj);
				List dataTypeList = (List) columnsObj.get("dataTypesList");
				List columnsList = (List) columnsObj.get("columnsList");
				fromColumnsList = columnsList;
				toColumnsList = columnsList;
				componentUtilities.createTable(toTableName, columnsList, dataTypeList, (Connection) toConnection);

				if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
					fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

				}

				if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {

					if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
							&& fromConnObj.containsKey("HOST_NAME")
							&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
							&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
							&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
							&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
							&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

						insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsList,
								toColumnsList, (Connection) toConnection, query, jobId);

						try {
							componentUtilities.processETLLog(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									"Processed " + insertCount + " Records", "INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}
					} else {
						String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
						preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

						long startInsertTime = System.currentTimeMillis();
						JSONObject infoObject = new JSONObject();
						// infoObject.put("skipRejectedRecords", skipRejectedRecords);
						infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
								preparedStatement, toColumnsList, fromDataList, jobId, infoObject);
						insertCount = (int) infoObject.get("insertCount");
						System.out.println(
								"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");

						try {
							componentUtilities.processETLLog(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									"Processed " + insertCount + " Records", "INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}
					}

				}

			} else if (query != null && query.contains("BEGIN")) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
				callableStmt = fromConnection.prepareCall(query);
				callableStmt.execute();
				System.out.println("Script executed Successfully");
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Script executed Successfully", "INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}
			} else {

				String[] queryArray = query.split(";");
				int executeCount = 0;
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
				for (int i = 0; i < queryArray.length; i++) {
					String singleQuery = queryArray[i];
					executeCount += processJobComponentsDAO.executeUpdateQuery(request, singleQuery, fromConnection,
							jobId);

				}
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Processed " + executeCount + " Records", "INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}

				JSONArray skipOperatorIds = getConnectedToOperatorIds(request, String.valueOf(operatorId),
						flowchartData);
				request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
						skipOperatorIds);

			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {

				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (fromConnection != null) {
					fromConnection.close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
				if (currentConnection != null) {
					currentConnection.close();
				}
			} catch (Exception e) {
			}
		}
		return operators;
	}

	public Map processJoinsComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		Connection currentConnection = null;
		PreparedStatement preparedStatement = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorIdsArray = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorIdsArray = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);

			JSONObject firstFromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorIdsArray.get(0)));
			JSONObject firstFromConnObj = (JSONObject) firstFromOperator.get("connObj");
			boolean sameFromConn = fromOperatorIdsArray.stream().allMatch(e -> {
				JSONObject op = (JSONObject) operators.get(String.valueOf(e));
				JSONObject connObject = (JSONObject) op.get("connObj");
				if (connObject != null && firstFromConnObj != null && connObject.containsKey("HOST_NAME")
						&& firstFromConnObj.containsKey("HOST_NAME")
						&& connObject.get("HOST_NAME").equals(firstFromConnObj.get("HOST_NAME"))
						&& connObject.get("CONN_PORT").equals(firstFromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(firstFromConnObj.get("CONN_USER_NAME"))
						&& connObject.get("CONN_PASSWORD").equals(firstFromConnObj.get("CONN_PASSWORD"))
						&& connObject.get("CONN_CUST_COL1").equals(firstFromConnObj.get("CONN_CUST_COL1"))
						&& connObject.get("CONN_DB_NAME").equals(firstFromConnObj.get("CONN_DB_NAME"))) {
					return true;
				} else {
					return false;
				}
			});
			boolean sameFromAndToConn = false;
			if (sameFromConn && toConnObj != null && firstFromConnObj != null && toConnObj.containsKey("HOST_NAME")
					&& firstFromConnObj.containsKey("HOST_NAME")
					&& toConnObj.get("HOST_NAME").equals(firstFromConnObj.get("HOST_NAME"))
					&& toConnObj.get("CONN_PORT").equals(firstFromConnObj.get("CONN_PORT"))
					&& toConnObj.get("CONN_USER_NAME").equals(firstFromConnObj.get("CONN_USER_NAME"))
					&& toConnObj.get("CONN_PASSWORD").equals(firstFromConnObj.get("CONN_PASSWORD"))
					&& toConnObj.get("CONN_CUST_COL1").equals(firstFromConnObj.get("CONN_CUST_COL1"))
					&& toConnObj.get("CONN_DB_NAME").equals(firstFromConnObj.get("CONN_DB_NAME"))) {
				sameFromAndToConn = true;
			}
			JSONObject currentConnObject = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName,
					password, "Current_V10");

			List fromOperatorList = (List) fromOperatorIdsArray.stream()
					.map(opId -> operators.get(String.valueOf(opId))).collect(Collectors.toList());
			List updatedFromOperatorsList = new ArrayList();
			List fromTablesList = new ArrayList();
			List allTablesColumns = new ArrayList();
			List aliasTablesColumns = new ArrayList();
			List allTablesDataTypes = new ArrayList();
			JSONObject aliasColumnsObj = new JSONObject();
			JSONObject originalTableNamesObj = new JSONObject();

			List fromColumnsList = new ArrayList();

			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			for (int i = 0; i < fromOperatorList.size(); i++) {

				JSONObject fromOperator = (JSONObject) fromOperatorList.get(i);
				String fromOperatorId = String.valueOf(fromOperator.get("operatorId"));
				JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
				String originalTabName = "";
				if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
					originalTabName = (String) fromOperator.get("tableName");

					if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
							|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
						// sap code
						fromOperator = sapToLocalTable(request, fromOperator, jobId);
					} else {
						// db
						if (!sameFromConn) {

							fromOperator = otherDBToLocalTable(request, fromOperator, jobId);

						}
					}
				} else {
					// files
					originalTabName = (String) fromConnObj.get("fileName");
					originalTabName = originalTabName.replaceAll("[^a-zA-Z0-9]", "_");
					fromOperator = fileToLocalTable(request, fromOperator, jobId);
				}
				operators.put(String.valueOf(fromOperatorId), fromOperator);
				originalTableNamesObj.put(originalTabName, fromOperator.get("tableName"));
				fromConnObj = (JSONObject) fromOperator.get("connObj");
				if (fromConnObj != null && !fromConnObj.isEmpty()) {
					fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
				}
				updatedFromOperatorsList.add(fromOperator);
				fromTablesList.add(fromOperator.get("tableName"));
				String tableName = (String) fromOperator.get("statusLabel");
				String originalTableName = (String) fromOperator.get("originalTableName");

			}
			JSONObject trfmRulesDataMap = (JSONObject) tooperator.get("trfmRules");
			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {
				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");

					if (destinationColumn.contains("/")) {
						if (destinationColumn.contains(":")) {
							destinationColumn = destinationColumn.split(":")[0] + ":" + "\""
									+ destinationColumn.split(":")[1] + "\"";
						} else {
							destinationColumn = "\"" + destinationColumn.split(":")[1] + "\"";
						}
					}

					if (destinationColumn != null && destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];

					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {

						if (sourceColumn.contains("/")) {
							if (sourceColumn.contains(":")) {
								sourceColumn = sourceColumn.split(":")[0] + ":" + "\"" + sourceColumn.split(":")[1]
										+ "\"";
							} else {
								sourceColumn = "\"" + sourceColumn.split(":")[1] + "\"";
							}
						}

						if (sourceColumn.contains(":")) {

							sourceColumn = sourceColumn.replaceAll(":", ".");

						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						fromColumnsList.addAll(funcolumnslist);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
					}

					toColumnsList.add(destinationColumn);
				}
			}

			String joinQuery = "";

			List<String> joinClauseDataMapList = (List) trfmRulesDataMap.get("joinClauseData");
			List<String> childTables = (List) trfmRulesDataMap.get("childTables");
			if (childTables != null && !childTables.isEmpty() && joinClauseDataMapList != null
					&& !joinClauseDataMapList.isEmpty()) {
				JSONObject joinQueryMapObj = new JSONObject();
				LinkedHashMap joinQueryHashMapObj = new LinkedHashMap();

				String masterTableName = (String) trfmRulesDataMap.get("masterTableName");
				joinQuery += " " + masterTableName;
				joinQueryMapObj.put(masterTableName, masterTableName);
				joinQueryHashMapObj.put(masterTableName, masterTableName);
				for (int i = 0; i < childTables.size(); i++) {
					String childTableName = childTables.get(i);
					String childJoinStr = joinClauseDataMapList.get(i);
					if (childJoinStr != null && !"".equalsIgnoreCase(childJoinStr)
							&& !"null".equalsIgnoreCase(childJoinStr)) {
						JSONObject joinObj = (JSONObject) JSONValue.parse(childJoinStr);
						if (joinObj != null && !joinObj.isEmpty()) {
							joinQueryMapObj.put(childTableName, joinObj);
							joinQueryHashMapObj.put(childTableName, joinObj);
							int j = 0;
							for (Object joinObjKey : joinObj.keySet()) {
								JSONObject joinMappedColumnObj = (JSONObject) joinObj.get(joinObjKey);
								if (joinMappedColumnObj != null && !joinMappedColumnObj.isEmpty()) {
									String childTableColumn = "";
									if (joinMappedColumnObj.get("childTableColumn") != null
											&& !"".equalsIgnoreCase(
													String.valueOf(joinMappedColumnObj.get("childTableColumn")))
											&& !"null".equalsIgnoreCase(
													String.valueOf(joinMappedColumnObj.get("childTableColumn")))) {// childTableColumn
//                                        childTableColumn = String.valueOf(joinMappedColumnObj.get("childTableColumn"));
										childTableColumn = String
												.valueOf(joinMappedColumnObj.get("childTableColumnActualValue"));
//                                        String childFunColumnsListStr = (String) joinMappedColumnObj.get("childFunColumnsList");
//                                        JSONArray childFunColumnsList = (JSONArray) JSONValue.parse(childFunColumnsListStr);
//                                        fromColumnsSet.addAll((List)childFunColumnsList.stream().map(col -> String.valueOf(col).contains(":") ? String.valueOf(col).replaceAll(":",".") : col).collect(Collectors.toList()));
//                                        toColumnsSet.addAll((List)childFunColumnsList.stream().map(col -> String.valueOf(col).contains(":") ? String.valueOf(col).split(":")[1] : col).collect(Collectors.toList()));
									}
									String masterTableColumn = "";
									if (joinMappedColumnObj.get("masterTableColumn") != null
											&& !"".equalsIgnoreCase(
													String.valueOf(joinMappedColumnObj.get("masterTableColumn")))
											&& !"null".equalsIgnoreCase(
													String.valueOf(joinMappedColumnObj.get("masterTableColumn")))) {// childTableColumn
//                                        masterTableColumn = String.valueOf(joinMappedColumnObj.get("masterTableColumn"));
										masterTableColumn = String
												.valueOf(joinMappedColumnObj.get("masterTableColumnActualValue"));
//                                        String masterFunColumnsListStr = (String) joinMappedColumnObj.get("masterFunColumnsList");
//                                        JSONArray masterFunColumnsList = (JSONArray) JSONValue.parse(masterFunColumnsListStr);
//                                        fromColumnsSet.addAll((List)masterFunColumnsList.stream().map(col -> String.valueOf(col).contains(":") ? String.valueOf(col).replaceAll(":",".") : col).collect(Collectors.toList()));
//                                        toColumnsSet.addAll((List)masterFunColumnsList.stream().map(col -> String.valueOf(col).contains(":") ? String.valueOf(col).split(":")[1] : col).collect(Collectors.toList()));
									}
									if (j == 0) {
										joinQuery += " " + joinMappedColumnObj.get("joinType") + " " + childTableName
												+ " ON ";
									}
									String colValue = String.valueOf(joinMappedColumnObj.get("staticValue"));
									if (colValue != null && !"".equalsIgnoreCase(colValue)
											&& !"null".equalsIgnoreCase(colValue)) {
										if (joinMappedColumnObj.get("operator") != null
												&& !"".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("operator")))
												&& !"null".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("operator")))
												&& ("IN".equalsIgnoreCase(
														String.valueOf(joinMappedColumnObj.get("operator")))
														|| "NOT IN".equalsIgnoreCase(
																String.valueOf(joinMappedColumnObj.get("operator"))))) {
											colValue = "('" + colValue.replaceAll("##", "','") + "')";
										} else {
											colValue = "'" + colValue + "'";
										}

									}

									joinQuery += " " + childTableColumn + " " + joinMappedColumnObj.get("operator")
											+ " " + " "
											+ ((joinMappedColumnObj.get("staticValue") != null
													&& !"".equalsIgnoreCase(
															String.valueOf(joinMappedColumnObj.get("staticValue")))
													&& !"null".equalsIgnoreCase(
															String.valueOf(joinMappedColumnObj.get("staticValue"))))
																	? "" + colValue + ""
																	: masterTableColumn);
									if (j != joinObj.size() - 1) {
										joinQuery += " AND ";
									}
								}
								j++;
							}
						}
					}
				}
			}

			List fromColumnsAliasList = new ArrayList();
//            fromColumnsList = new ArrayList(fromColumnsSet);
//            toColumnsList = new ArrayList(toColumnsSet);

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);

				if (i < (fromColumnsList.size() - 1)) {
					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}

				fromColumnsAliasList.add(toColumn);
			}

			for (Object originalTable : originalTableNamesObj.keySet()) {
				String actualTable = (String) originalTableNamesObj.get(originalTable);
				if (actualTable != null && !"".equalsIgnoreCase(actualTable)) {
					joinQuery = joinQuery.replaceAll(originalTable + ":", actualTable + ".");
					joinQuery = joinQuery.replaceAll(originalTable + " ", actualTable + " ");
					columnsListStr = columnsListStr.replaceAll(originalTable + ".", actualTable + ".");
				} else {
					joinQuery = joinQuery.replaceAll(originalTable + ":", originalTable + ".");
					joinQuery = joinQuery.replaceAll(originalTable + " ", originalTable + " ");
					columnsListStr = columnsListStr.replaceAll(originalTable + ".", originalTable + ".");
				}

			}
			String query = "SELECT " + columnsListStr + " FROM " + joinQuery;
			System.out.println("query :: " + query);
			String selectQuery = "SELECT * FROM (" + query + ") WHERE 1=2";

			System.out.println("query :: " + query);
			List dataTypesList = new ArrayList();
			List columnsList = new ArrayList();

			if (sameFromConn) {
				JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(selectQuery, fromConnection,
						firstFromConnObj);
				dataTypesList = (List) columnsObject.get("dataTypesList");
				columnsList = (List) columnsObject.get("columnsList");
			} else {
				JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(selectQuery, fromConnection,
						currentConnObject);
				dataTypesList = (List) columnsObject.get("dataTypesList");
				columnsList = (List) columnsObject.get("columnsList");
			}

			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);
			List fromDataList = new ArrayList();
			if (!sameFromAndToConn) {
				if (sameFromConn) {
					fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, firstFromConnObj,
							jobId);
				} else {
					fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, currentConnObject,
							jobId);
				}

			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (sameFromAndToConn) {

					insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsAliasList,
							toColumnsList, toConnection, query, jobId);

					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					insertCount = (int) infoObject.get("insertCount");
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (fromConnection != null) {
					fromConnection.close();
				}
				if (toConnection != null) {
					toConnection.close();
				}
				if (currentConnection != null) {
					currentConnection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}

	public Map processDenormalizeComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) { // updated getDeNoramlisedData Ravi

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");
			String originalTableName = fromOperator.get("originalTableName") != null
					? (String) fromOperator.get("originalTableName")
					: null;

			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;
			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);
			fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");
			JSONObject normalizeOptionsObj = (JSONObject) trfmRules.get("normalizeOptionsObj");
			String deNormalizeColumn = (String) normalizeOptionsObj.get("denormalizeColumn");
			String delimiter = (String) normalizeOptionsObj.get("delimiter");
			String keyColumn = (String) normalizeOptionsObj.get("keyColumn");

			String wrtColumn = keyColumn;
			String wrtColumnVal = "";
			String prevWrtColumn = "";
			String prevWrtColumnVal = "";
			int wrtColIndex = 0;
			int deNormalizeColumnIndex = 0;
			JSONArray groupingVals = new JSONArray();
//            Map prevRowDataObj = new HashMap();
			Object[] prevRowDataObj = new Object[fromColumnsList.size()];
			List sortedFileData = new ArrayList();
			List deNormalisedData = new ArrayList();

			if (fromDataList != null && !fromDataList.isEmpty()) {
				int index = 0;
				for (Object key : fromColumnsList) {
//                    if (index == 0 && !((String) key).equalsIgnoreCase(deNormalizeColumn)) {
//                        wrtColumn = (String) key;
//                        wrtColIndex = index;
//                    }
//                    if (index == 1 && "".equalsIgnoreCase(wrtColumn)) {
//                        wrtColumn = (String) key;
//                        wrtColIndex = index;
//                    }
					if (String.valueOf(key).equalsIgnoreCase(deNormalizeColumn)) {
						deNormalizeColumnIndex = index;
					}
					if (String.valueOf(key).equalsIgnoreCase(keyColumn)) {
						wrtColIndex = index;
					}
					index++;
				}

				// for (int i = 0; i < fileData.size(); i++) {
				int count = 0;
				while (count < fromDataList.size()) {

					Object[] rowDataObj = (Object[]) fromDataList.get(count);
					count++;
					wrtColumnVal = (String) rowDataObj[wrtColIndex];

					for (int j = 0; j < fromDataList.size(); j++) {

						Object[] newRowDataObj = (Object[]) fromDataList.get(j);
						String newWrtColumnVal = (String) newRowDataObj[wrtColIndex];
						if (newWrtColumnVal != null && newWrtColumnVal.equalsIgnoreCase(wrtColumnVal)) {
							sortedFileData.add(newRowDataObj);
							fromDataList.remove(j);
							j--;
							count = 0;
						}
					}

				}

				for (int i = 0; i < sortedFileData.size(); i++) {
//                    Map rowDataObj = (Map) sortedFileData.get(i);
					Object[] rowDataObj = (Object[]) sortedFileData.get(i);

					if (i == 0) {

						wrtColumnVal = (String) rowDataObj[wrtColIndex];
						groupingVals.add(rowDataObj[deNormalizeColumnIndex]);
						prevWrtColumnVal = wrtColumnVal;
						prevRowDataObj = rowDataObj;

					} else {
						wrtColumnVal = (String) rowDataObj[wrtColIndex];
						if (!prevWrtColumnVal.equalsIgnoreCase(wrtColumnVal)) {
//                            Map newRowData = new HashMap(prevRowDataObj);
							Object[] newRowData = Arrays.stream(prevRowDataObj).map(e -> e).toArray(Object[]::new);
							String groupingValsStr = String.join(delimiter, groupingVals);
							groupingVals.clear();
							groupingVals.add(rowDataObj[deNormalizeColumnIndex]);
							newRowData[deNormalizeColumnIndex] = groupingValsStr;
							deNormalisedData.add(newRowData);
							prevWrtColumnVal = wrtColumnVal;
							prevRowDataObj = rowDataObj;
						} else {
							groupingVals.add(rowDataObj[deNormalizeColumnIndex]);
						}

						if (i == sortedFileData.size() - 1) {
							String groupingValsStr = String.join(delimiter, groupingVals);
							rowDataObj[deNormalizeColumnIndex] = groupingValsStr;
							deNormalisedData.add(rowDataObj);
							groupingVals.clear();
						}
					}

					// prevWrtColumnVal = wrtColumnVal;
				}
			}

			String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
			PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

			long startInsertTime = System.currentTimeMillis();
			JSONObject infoObject = new JSONObject();
			// infoObject.put("skipRejectedRecords", skipRejectedRecords);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
					toColumnsList, deNormalisedData, jobId, infoObject);
			insertCount = (int) infoObject.get("insertCount");
			System.out.println("Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
			long jobEndTime = System.currentTimeMillis();
			long jobStartTime = (long) request.getAttribute("jobStartTime");
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"),
						"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
			} catch (Exception ex) {
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processNormalizeComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");
			String originalTableName = fromOperator.get("originalTableName") != null
					? (String) fromOperator.get("originalTableName")
					: null;

			String query = "SELECT * FROM " + fromTable;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;
			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);
			fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			JSONObject normalizeOptionsObj = (JSONObject) trfmRules.get("normalizeOptionsObj");
			String normalizeColumn = (String) normalizeOptionsObj.get("normalizeColumn");
			String itemSeparator = (String) normalizeOptionsObj.get("itemSeparator");
			int normalizeColumnIndex = fromColumnsList.indexOf(normalizeColumn.toUpperCase());

			ArrayList normalisedData = new ArrayList();
			for (int i = 0; i < fromDataList.size(); i++) {
				Object[] rowData = (Object[]) fromDataList.get(i);
				String groupedDataColValue = (String) rowData[normalizeColumnIndex];
				String[] groupedDataArray = groupedDataColValue.split(itemSeparator);
				for (int j = 0; j < groupedDataArray.length; j++) {
					String splitedVal = groupedDataArray[j];
					Object[] newRowData = Arrays.stream(rowData).map(e -> e).toArray(Object[]::new);
					newRowData[normalizeColumnIndex] = splitedVal;
					normalisedData.add(newRowData);
				}
			}

			String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
			PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

			long startInsertTime = System.currentTimeMillis();
			JSONObject infoObject = new JSONObject();
			// infoObject.put("skipRejectedRecords", skipRejectedRecords);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
					toColumnsList, normalisedData, jobId, infoObject);

			insertCount = (int) infoObject.get("insertCount");
			System.out.println("Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
			long jobEndTime = System.currentTimeMillis();
			long jobStartTime = (long) request.getAttribute("jobStartTime");
			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"),
						"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
			} catch (Exception ex) {
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return operators;
	}

	public String generateInsertQuery(String tableName, List<String> columnsList) {
		String query = "";
		try {
			String columnsStr = (String) columnsList.stream().map(col -> {
				if (col.contains("/")) {
					col = "\"" + col + "\"";
				}
				col = col.replaceAll(":", ".");
				return col;
			}).collect(Collectors.joining(","));
			String paramsStr = (String) columnsList.stream().map(e -> "?").collect(Collectors.joining(","));
			query = " INSERT INTO " + tableName + " (" + columnsStr + ")" + " VALUES (" + paramsStr + ")";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return query;
	}

	public boolean alterTableSCDType3(Map operator, Connection connection, JSONArray historyCols) {
		boolean tableAltered = false;
		PreparedStatement preparedStatement = null;
//        Connection connection = null;
		try {
			JSONObject connObj = (JSONObject) operator.get("connObj");
			Object connectionObj = componentUtilities.getConnection(connObj);
			if (connectionObj instanceof Connection) {
				connection = (Connection) connectionObj;
			}

			String destTable = String.valueOf(operator.get("tableName"));
			if (destTable.contains(".")) {
				destTable = destTable.split("\\.")[1];
			}
			List<Object[]> fromColumnsObjList = componentUtilities.getTableColumnsOpt(connection, (JSONObject) connObj,
					destTable);
			List colsList = fromColumnsObjList.stream().map(e -> (String) ((Object[]) e)[2])
					.collect(Collectors.toList());
			List dataTypesList = fromColumnsObjList.stream().map(e -> (String) ((Object[]) e)[8])
					.collect(Collectors.toList());
			String alterQuery = "ALTER TABLE " + destTable + " ADD (";
			for (int i = 0; i < colsList.size(); i++) {
				String colName = String.valueOf(colsList.get(i));
				if (historyCols.contains(colName)) {
					alterQuery += " PREV_" + colName + " " + dataTypesList.get(i) + ",";
				}
			}
			alterQuery = alterQuery.substring(0, alterQuery.length() - 1) + " )";
//            String alterQuery = "ALTER TABLE " + destTable + " ADD (ADDRESS_KEY VARCHAR2(200), FLAG VARCHAR2(4), START_DATE DATE, END_DATE DATE)";
			preparedStatement = connection.prepareStatement(alterQuery);
			tableAltered = preparedStatement.execute();

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
//                if (connection != null) {
//                    connection.close();
//                }

			} catch (Exception e) {
			}
		}
		return tableAltered;
	}

	public boolean alterTableSCDType2(Map operator, Connection connection) {
		boolean tableAltered = false;
		PreparedStatement preparedStatement = null;
//        Connection connection = null;
		try {
			JSONObject connObj = (JSONObject) operator.get("connObj");
			Object connectionObj = componentUtilities.getConnection(connObj);
			if (connectionObj instanceof Connection) {
				connection = (Connection) connectionObj;
			}
			String destTable = String.valueOf(operator.get("tableName"));
			if (destTable.contains(".")) {
				destTable = destTable.split("\\.")[1];
			}
			try {

				String alterQuery = "ALTER TABLE " + destTable
						+ " ADD (ADDRESS_KEY VARCHAR2(200), FLAG VARCHAR2(4), START_DATE DATE, END_DATE DATE)";
				preparedStatement = connection.prepareStatement(alterQuery);
				tableAltered = preparedStatement.execute();
			} catch (Exception e) {
				e.printStackTrace();
			}
			JSONArray primaryKeys = componentUtilities.getPrimaryKeyColumns(connection, destTable);
			if (!primaryKeys.contains("ADDRESS_KEY")) {
				primaryKeys.add("ADDRESS_KEY");
				String pkString = (String) primaryKeys.stream().map(e -> e).collect(Collectors.joining(","));
				try {
					String alterQueryDropPk = "ALTER TABLE " + destTable + " DROP PRIMARY KEY";
					preparedStatement = connection.prepareStatement(alterQueryDropPk);
					tableAltered = preparedStatement.execute();

				} catch (Exception e) {
					e.printStackTrace();

				}
				try {
					String alterQueryPk = "ALTER TABLE " + destTable + " ADD CONSTRAINT " + destTable
							+ "_PK PRIMARY KEY (" + pkString + ")";
					preparedStatement = connection.prepareStatement(alterQueryPk);
					tableAltered = preparedStatement.execute();
				} catch (Exception e) {
					e.printStackTrace();

				}
			}

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
//                if (connection != null) {
//                    connection.close();
//                }

			} catch (Exception e) {
			}
		}
		return tableAltered;
	}

	public Map processPivotComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		Connection currentConnection = null;
		ResultSet resultset = null;
		PreparedStatement preparedStatement = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {
			List pivotDataList = new ArrayList();

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
//            String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}
			currentConnection = componentUtilities.getCurrentConnection();
//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromTable = (String) fromOperator.get("tableName");

			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRulesDataMap = (JSONObject) tooperator.get("trfmRules");

			String pivotColumnLabel = String.valueOf(trfmRulesDataMap.get("pivotColumnLabel"));
			if (pivotColumnLabel != null && !"null".equalsIgnoreCase(pivotColumnLabel)
					&& !"".equalsIgnoreCase(pivotColumnLabel)) {
				String createTableName = String.valueOf(trfmRulesDataMap.get("createTableName"));
				JSONArray columnsList = new JSONArray();
				columnsList.add(pivotColumnLabel);

				String query = "SELECT DISTINCT " + pivotColumnLabel + " FROM " + fromTable;
				List columnDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, "");
				String columnLablesStr = (String) columnDataList.stream().map(e -> String.valueOf(e))
						.collect(Collectors.joining(","));

				List dataTypesList = (List) columnDataList.stream().map(e -> "NUMBER").collect(Collectors.toList());

				String pivotAggFun = String.valueOf(trfmRulesDataMap.get("pivotAggFun"));

				String pivotRowLabel = String.valueOf(trfmRulesDataMap.get("pivotRowLabel"));
				if (pivotRowLabel != null && !"null".equalsIgnoreCase(pivotRowLabel)
						&& !"".equalsIgnoreCase(pivotRowLabel)) {
					columnDataList.add(0, pivotRowLabel);
					dataTypesList.add(0, "VARCHAR2(200)");
				}
				componentUtilities.dropStagingTable(createTableName, currentConnection);
				boolean tableCreated = componentUtilities.createTable(createTableName, columnDataList, dataTypesList,
						currentConnection);
				if (tableCreated) {
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								createTableName + " table Created", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}

				String pivotColumnValue = String.valueOf(trfmRulesDataMap.get("pivotColumnValue"));
				if (pivotColumnValue != null && !"null".equalsIgnoreCase(pivotColumnValue)
						&& !"".equalsIgnoreCase(pivotColumnValue)) {

					String pivotCols = pivotColumnValue;
					if (pivotRowLabel != null && !"null".equalsIgnoreCase(pivotRowLabel)
							&& !"".equalsIgnoreCase(pivotRowLabel)) {
						pivotCols = pivotRowLabel + "," + pivotColumnValue;
					}
					String pivotQuery = "SELECT * FROM ( SELECT " + pivotCols + " FROM " + fromTable + ") " + "PIVOT "
							+ "( COUNT(" + pivotColumnValue + ") FOR (" + pivotColumnValue + ") IN (" + columnLablesStr
							+ ") ) ";

					if (pivotRowLabel != null && !"null".equalsIgnoreCase(pivotRowLabel)
							&& !"".equalsIgnoreCase(pivotRowLabel)) {
						pivotQuery += " ORDER BY " + pivotRowLabel;
					}
					System.out.println("pivotQuery :: " + pivotQuery);
					preparedStatement = currentConnection.prepareStatement(pivotQuery);
					resultset = preparedStatement.executeQuery();
					while (resultset.next()) {
						Object[] rowData = new Object[columnDataList.size()];
						for (int i = 0; i < columnDataList.size(); i++) {
							rowData[i] = resultset.getObject(i + 1);
						}
						pivotDataList.add(rowData);
					}

					String insertQuery = generateInsertQuery(createTableName, columnDataList);
					System.out.println("insertQuery :: " + insertQuery);
					preparedStatement = currentConnection.prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, createTableName,
							preparedStatement, columnDataList, pivotDataList, jobId, infoObject);
					insertCount = (int) infoObject.get("insertCount");
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities
								.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										"Inserted " + insertCount + " Records into " + createTableName + " in "
												+ ((jobEndTime - jobStartTime) / 1000) + " Sec",
										"INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

//            resultObj.put("insertCount", insertCount);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public Map processUnpivotComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		Connection currentConnection = null;
		ResultSet resultset = null;
		PreparedStatement preparedStatement = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {
			List pivotDataList = new ArrayList();

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
//            String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}
			currentConnection = componentUtilities.getCurrentConnection();
//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromTable = (String) fromOperator.get("tableName");

			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRulesDataMap = (JSONObject) tooperator.get("trfmRules");

			String createTableName = String.valueOf(trfmRulesDataMap.get("createTableName"));
			String unpivotColumnsLabel = String.valueOf(trfmRulesDataMap.get("unpivotColumnsLabel"));
			String unpivotValuesLabel = String.valueOf(trfmRulesDataMap.get("unpivotValuesLabel"));

			JSONArray fromTableColumns = new JSONArray();
			JSONArray unpivotColumns = new JSONArray();
			if (trfmRulesDataMap.get("unpivotColumns") != null) {
				unpivotColumns = (JSONArray) trfmRulesDataMap.get("unpivotColumns");
				if (trfmRulesDataMap.get("fromTableColumns") != null) {
					String fromTableColumnsStr = (String) trfmRulesDataMap.get("fromTableColumns");
					fromTableColumns = (JSONArray) JSONValue.parse(fromTableColumnsStr);
//                    fromTableColumns = (JSONArray) trfmRulesDataMap.get("fromTableColumns");
				}
				fromTableColumns.removeAll(unpivotColumns);

				fromTableColumns.add(unpivotColumnsLabel);
				fromTableColumns.add(unpivotValuesLabel);

				List dataTypesList = (List) fromTableColumns.stream().map(e -> "VARCHAR2(1000)")
						.collect(Collectors.toList());
				String unpivotColsStr = (String) unpivotColumns.stream().map(e -> e).collect(Collectors.joining(","));
				componentUtilities.dropStagingTable(createTableName, currentConnection);
				boolean tableCreated = componentUtilities.createTable(createTableName, fromTableColumns, dataTypesList,
						currentConnection);
				if (tableCreated) {
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								createTableName + " table Created", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}

				String unpivotQuery = "SELECT * FROM " + fromTable + " " + "UNPIVOT ( " + unpivotValuesLabel + " FOR "
						+ unpivotColumnsLabel + " IN (" + unpivotColsStr + " ) )";

				System.out.println("unpivotQuery :: " + unpivotQuery);
				preparedStatement = currentConnection.prepareStatement(unpivotQuery);
				resultset = preparedStatement.executeQuery();
				while (resultset.next()) {
					Object[] rowData = new Object[fromTableColumns.size()];
					for (int i = 0; i < fromTableColumns.size(); i++) {
						rowData[i] = resultset.getObject(i + 1);
					}
					pivotDataList.add(rowData);
				}

				String insertQuery = generateInsertQuery(createTableName, fromTableColumns);
				System.out.println("insertQuery :: " + insertQuery);
				preparedStatement = currentConnection.prepareStatement(insertQuery);

				long startInsertTime = System.currentTimeMillis();
				JSONObject infoObject = new JSONObject();
				// infoObject.put("skipRejectedRecords", skipRejectedRecords);
				infoObject = processJobComponentsDAO.insertDataIntoTable(request, createTableName, preparedStatement,
						fromTableColumns, pivotDataList, jobId, infoObject);
				insertCount = (int) infoObject.get("insertCount");
				System.out.println("Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
				long jobEndTime = System.currentTimeMillis();
				long jobStartTime = (long) request.getAttribute("jobStartTime");
				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Inserted " + insertCount + " Records into " + createTableName + " in "
									+ ((jobEndTime - jobStartTime) / 1000) + " Sec",
							"INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}
			}

//            resultObj.put("insertCount", insertCount);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;
	}

	public JSONObject processGroupJobData(HttpServletRequest request, JSONObject flowchartData, String jobId) {
		final int numberOfThreads = 50;
		final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		final List<Future<?>> futures = new ArrayList<>();
		Connection currConnection = null;
		JSONObject resultObj = new JSONObject();
		List dropTablesList = new ArrayList();
		asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--"
				+ request.getSession(false).getAttribute("ssOrgId"), executor);

		request.setAttribute("stagingTablesList", new ArrayList());
		try {
			try {
				componentUtilities.deleteProcesslog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				componentUtilities.deleteProcessBarlog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
			}
			try {
				componentUtilities.deleteJobProcessSteps((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
			}

			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "Started Job Execution", "INFO", 20,
						"Y", jobId);
			} catch (Exception e) {
				e.printStackTrace();
			}

			request.setAttribute("jobStartTime", System.currentTimeMillis());

			JSONObject operatorsObj = (JSONObject) flowchartData.get("operators");
			Map operators1 = new HashedMap();
			operators1.putAll(operatorsObj);
			Map operators = componentUtilities.sortOperators(operators1);

			String nextExecutionSeq = "";
			List<String> keyList = new ArrayList(operators.keySet());
			for (Object operatorId : keyList) {

				JSONObject operator = (JSONObject) operators.get(operatorId);
				String currentExecutionSeq = String.valueOf(operator.get("executionSequence"));
				int operatorIndex = keyList.indexOf(operatorId);
				if (operatorIndex != (keyList.size() - 1)) {
					String nextOperatorId = keyList.get(operatorIndex + 1);
					JSONObject nextOperator = (JSONObject) operators.get(nextOperatorId);
					nextExecutionSeq = String.valueOf(nextOperator.get("executionSequence"));
				} else {
					nextExecutionSeq = "";
				}

				String currentOperatorId = String.valueOf(operatorId);
				if (nextExecutionSeq != null && !"".equalsIgnoreCase(nextExecutionSeq)
						&& nextExecutionSeq.equalsIgnoreCase(currentExecutionSeq)) {
					futures.add(executor.submit(() -> {

						try {
							componentUtilities.processETLLogSteps(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									String.valueOf(operatorId), "START", "", "Y", jobId);
						} catch (Exception ex) {
						}

						String threadName = Thread.currentThread().getName();
						System.out.println("threadName  - " + Thread.currentThread().getId());
//                        asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--" + request.getSession(false).getAttribute("ssOrgId") + AuditIdGenerator.genRandom32Hex(), Thread.currentThread());
						if (operator != null && operator.size() > 0) {
							String subJobId = String.valueOf(operator.get("jobId"));
							String subJobDesc = String.valueOf(operator.get("jobDesc"));
							JSONObject jobFlowChartDataObj = processJobComponentsDAO.getJobTransformationRules(request,
									subJobId);
							String jobFlowChartDataStr = (String) jobFlowChartDataObj.get("mappedObjectData");
//                        MutableHttpRequest mHReq = new MutableHttpRequest(request);
////                         request.getParameter("flowchartData");
//                        mHReq.addParameter("flowchartData", jobFlowChartDataStr);
							JSONObject jobFlowChartData = (JSONObject) JSONValue.parse(jobFlowChartDataStr);

							JSONObject jobResultObj = processJobComponents(request, jobFlowChartData, subJobId);

							String resultFlag = String.valueOf(jobResultObj.get("resultFlag"));
							if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
									&& resultFlag.equalsIgnoreCase("Success")) {
								try {

									componentUtilities.processETLLog(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
								} catch (Exception ex) {
									ex.printStackTrace();
								}

								try {

									componentUtilities.processETLLogSteps(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											String.valueOf(operatorId), "END", "SUCCESS", "Y", jobId);
								} catch (Exception ex) {
								}

							} else if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
									&& resultFlag.equalsIgnoreCase("Fail")) {
								try {

									componentUtilities.processETLLog(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
								} catch (Exception ex) {
									ex.printStackTrace();
								}

								try {

									componentUtilities.processETLLogSteps(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											String.valueOf(operatorId), "END", "FAIL", "Y", jobId);
								} catch (Exception ex) {
								}

							}

							processOnSuccessOrFailExecJob(request, flowchartData, operator, subJobId, resultFlag);
						}

					}));
				} else {
					futures.add(executor.submit(() -> {

						try {
							componentUtilities.processETLLogSteps(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									String.valueOf(operatorId), "START", "", "Y", jobId);
						} catch (Exception ex) {
						}

						String threadName = Thread.currentThread().getName();
						System.out.println("threadName  - " + Thread.currentThread().getId());
//                        asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--" + request.getSession(false).getAttribute("ssOrgId") + AuditIdGenerator.genRandom32Hex(), Thread.currentThread());
						if (operator != null && operator.size() > 0) {
							String subJobId = String.valueOf(operator.get("jobId"));
							String subJobDesc = String.valueOf(operator.get("jobDesc"));
							JSONObject jobFlowChartDataObj = processJobComponentsDAO.getJobTransformationRules(request,
									subJobId);
							String jobFlowChartDataStr = (String) jobFlowChartDataObj.get("mappedObjectData");
//                        MutableHttpRequest mHReq = new MutableHttpRequest(request);
////                         request.getParameter("flowchartData");
//                        mHReq.addParameter("flowchartData", jobFlowChartDataStr);
							JSONObject jobFlowChartData = (JSONObject) JSONValue.parse(jobFlowChartDataStr);
							JSONObject jobResultObj = processJobComponents(request, jobFlowChartData, subJobId);
							String resultFlag = String.valueOf(jobResultObj.get("resultFlag"));
							if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
									&& resultFlag.equalsIgnoreCase("Success")) {
								try {

									componentUtilities.processETLLog(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
								} catch (Exception ex) {
									ex.printStackTrace();
								}
							} else if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
									&& resultFlag.equalsIgnoreCase("Fail")) {
								try {

									componentUtilities.processETLLog(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
								} catch (Exception ex) {
									ex.printStackTrace();
								}
							}

							try {
								String threadNewName = Thread.currentThread().getName();
								String stepStatus = threadNewName.endsWith("_ERROR") ? "FAIL" : "SUCCESS";
								componentUtilities.processETLLogSteps(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										String.valueOf(operatorId), "END", stepStatus, "Y", jobId);
							} catch (Exception ex) {
							}

							processOnSuccessOrFailExecJob(request, flowchartData, operator, subJobId, resultFlag);
						}

					}));

					if (futures != null && !futures.isEmpty()) {
						try {
							for (Future<?> future : futures) {
								future.get();
								// do anything you need, e.g. isDone(), ...
							}
							futures.clear();
						} catch (InterruptedException | ExecutionException e) {
							e.printStackTrace();
						}
					}
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			currConnection = componentUtilities.getCurrentConnection();
			List stagingTablesList = (List) request.getAttribute("stagingTablesList");
			for (int i = 0; i < stagingTablesList.size(); i++) {
				String dropTableName = (String) stagingTablesList.get(i);
				componentUtilities.dropStagingTable(dropTableName, currConnection);
			}
			try {
				if (currConnection != null) {
					currConnection.close();
				}
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "Job Execution Complete", "INFO",
						20, "N", jobId);
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			try {
				componentUtilities.processETLLogSteps((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "", "END", "", "N", jobId);
			} catch (Exception ex) {
			}

			try {
				if (asyncJobMap != null && !asyncJobMap.isEmpty()) {
//                    asyncJobMap.remove(jobId + (String) request.getSession(false).getAttribute("ssUsername") + request.getSession(false).getAttribute("ssOrgId"));
					String processId = jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--"
							+ request.getSession(false).getAttribute("ssOrgId");
					List<String> keySet = new ArrayList(asyncJobMap.keySet());
					for (String key : keySet) {
						if (key.contains(processId)) {
							asyncJobMap.remove(key);
						}
					}

				}
			} catch (Exception e) {
			}

		}
		return resultObj;
	}

	public JSONObject processGroupJobDataOld(HttpServletRequest request, JSONObject flowchartData, String jobId) {
		final int numberOfThreads = 50;
		final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		final List<Future<?>> futures = new ArrayList<>();
		Connection currConnection = null;
		JSONObject resultObj = new JSONObject();
		List dropTablesList = new ArrayList();
		asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--"
				+ request.getSession(false).getAttribute("ssOrgId"), executor);

		request.setAttribute("stagingTablesList", new ArrayList());
		try {
			try {
				componentUtilities.deleteProcesslog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				componentUtilities.deleteProcessBarlog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
			}
			try {
				componentUtilities.deleteJobProcessSteps((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId);
			} catch (Exception e) {
			}

			try {
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "Started Job Execution", "INFO", 20,
						"Y", jobId);
			} catch (Exception e) {
				e.printStackTrace();
			}

			request.setAttribute("jobStartTime", System.currentTimeMillis());

			JSONObject operatorsObj = (JSONObject) flowchartData.get("operators");
			Map operators1 = new HashedMap();
			operators1.putAll(operatorsObj);
			Map operators = componentUtilities.sortOperators(operators1);

			String nextExecutionSeq = "";
			List<String> keyList = new ArrayList(operators.keySet());
			for (Object operatorId : keyList) {

				JSONObject operator = (JSONObject) operators.get(operatorId);
				String currentExecutionSeq = String.valueOf(operator.get("executionSequence"));
				int operatorIndex = keyList.indexOf(operatorId);
				if (operatorIndex != (keyList.size() - 1)) {
					String nextOperatorId = keyList.get(operatorIndex + 1);
					JSONObject nextOperator = (JSONObject) operators.get(nextOperatorId);
					nextExecutionSeq = String.valueOf(nextOperator.get("executionSequence"));
				} else {
					nextExecutionSeq = "";
				}

				String currentOperatorId = String.valueOf(operatorId);
				if (nextExecutionSeq != null && !"".equalsIgnoreCase(nextExecutionSeq)
						&& nextExecutionSeq.equalsIgnoreCase(currentExecutionSeq)) {
					futures.add(executor.submit(() -> {

						try {
							componentUtilities.processETLLogSteps(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									String.valueOf(operatorId), "START", "", "Y", jobId);
						} catch (Exception ex) {
						}

						String threadName = Thread.currentThread().getName();
						System.out.println("threadName  - " + Thread.currentThread().getId());
//                        asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--" + request.getSession(false).getAttribute("ssOrgId") + AuditIdGenerator.genRandom32Hex(), Thread.currentThread());
						if (operator != null && operator.size() > 0) {
							String subJobId = String.valueOf(operator.get("jobId"));
							String subJobDesc = String.valueOf(operator.get("jobDesc"));
							JSONObject jobFlowChartDataObj = processJobComponentsDAO.getJobTransformationRules(request,
									subJobId);
							String jobFlowChartDataStr = (String) jobFlowChartDataObj.get("mappedObjectData");
//                        MutableHttpRequest mHReq = new MutableHttpRequest(request);
////                         request.getParameter("flowchartData");
//                        mHReq.addParameter("flowchartData", jobFlowChartDataStr);
							JSONObject jobFlowChartData = (JSONObject) JSONValue.parse(jobFlowChartDataStr);

							JSONObject jobResultObj = processJobComponents(request, jobFlowChartData, subJobId);

							String resultFlag = String.valueOf(jobResultObj.get("resultFlag"));
							if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
									&& resultFlag.equalsIgnoreCase("Success")) {
								try {

									componentUtilities.processETLLog(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
								} catch (Exception ex) {
									ex.printStackTrace();
								}

								try {

									componentUtilities.processETLLogSteps(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											String.valueOf(operatorId), "END", "SUCCESS", "Y", jobId);
								} catch (Exception ex) {
								}

							} else if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
									&& resultFlag.equalsIgnoreCase("Fail")) {
								try {

									componentUtilities.processETLLog(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
								} catch (Exception ex) {
									ex.printStackTrace();
								}

								try {

									componentUtilities.processETLLogSteps(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											String.valueOf(operatorId), "END", "FAIL", "Y", jobId);
								} catch (Exception ex) {
								}

							}

							processOnSuccessOrFailExecJob(request, flowchartData, operator, subJobId, resultFlag);
						}

					}));
				} else {
					futures.add(executor.submit(() -> {

						try {
							componentUtilities.processETLLogSteps(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									String.valueOf(operatorId), "START", "", "Y", jobId);
						} catch (Exception ex) {
						}

						String threadName = Thread.currentThread().getName();
						System.out.println("threadName  - " + Thread.currentThread().getId());
//                        asyncJobMap.put(jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--" + request.getSession(false).getAttribute("ssOrgId") + AuditIdGenerator.genRandom32Hex(), Thread.currentThread());
						if (operator != null && operator.size() > 0) {
							String subJobId = String.valueOf(operator.get("jobId"));
							String subJobDesc = String.valueOf(operator.get("jobDesc"));
							JSONObject jobFlowChartDataObj = processJobComponentsDAO.getJobTransformationRules(request,
									subJobId);
							String jobFlowChartDataStr = (String) jobFlowChartDataObj.get("mappedObjectData");
//                        MutableHttpRequest mHReq = new MutableHttpRequest(request);
////                         request.getParameter("flowchartData");
//                        mHReq.addParameter("flowchartData", jobFlowChartDataStr);
							JSONObject jobFlowChartData = (JSONObject) JSONValue.parse(jobFlowChartDataStr);
							JSONObject jobResultObj = processJobComponents(request, jobFlowChartData, subJobId);
							String resultFlag = String.valueOf(jobResultObj.get("resultFlag"));
							if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
									&& resultFlag.equalsIgnoreCase("Success")) {
								try {

									componentUtilities.processETLLog(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
								} catch (Exception ex) {
									ex.printStackTrace();
								}
							} else if (resultFlag != null && !"null".equalsIgnoreCase(resultFlag)
									&& resultFlag.equalsIgnoreCase("Fail")) {
								try {

									componentUtilities.processETLLog(
											(String) request.getSession(false).getAttribute("ssUsername"),
											(String) request.getSession(false).getAttribute("ssOrgId"),
											subJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
								} catch (Exception ex) {
									ex.printStackTrace();
								}
							}

							try {
								String threadNewName = Thread.currentThread().getName();
								String stepStatus = threadNewName.endsWith("_ERROR") ? "FAIL" : "SUCCESS";
								componentUtilities.processETLLogSteps(
										(String) request.getSession(false).getAttribute("ssUsername"),
										(String) request.getSession(false).getAttribute("ssOrgId"),
										String.valueOf(operatorId), "END", stepStatus, "Y", jobId);
							} catch (Exception ex) {
							}

							processOnSuccessOrFailExecJob(request, flowchartData, operator, subJobId, resultFlag);
						}

					}));

					if (futures != null && !futures.isEmpty()) {
						try {
							for (Future<?> future : futures) {
								future.get();
								// do anything you need, e.g. isDone(), ...
							}
							futures.clear();
						} catch (InterruptedException | ExecutionException e) {
							e.printStackTrace();
						}
					}
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			currConnection = componentUtilities.getCurrentConnection();
			List stagingTablesList = (List) request.getAttribute("stagingTablesList");
			for (int i = 0; i < stagingTablesList.size(); i++) {
				String dropTableName = (String) stagingTablesList.get(i);
				componentUtilities.dropStagingTable(dropTableName, currConnection);
			}
			try {
				if (currConnection != null) {
					currConnection.close();
				}
				componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "Job Execution Complete", "INFO",
						20, "N", jobId);
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			try {
				componentUtilities.processETLLogSteps((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), "", "END", "", "N", jobId);
			} catch (Exception ex) {
			}

			try {
				if (asyncJobMap != null && !asyncJobMap.isEmpty()) {
//                    asyncJobMap.remove(jobId + (String) request.getSession(false).getAttribute("ssUsername") + request.getSession(false).getAttribute("ssOrgId"));
					String processId = jobId + "--" + request.getSession(false).getAttribute("ssUsername") + "--"
							+ request.getSession(false).getAttribute("ssOrgId");
					List<String> keySet = new ArrayList(asyncJobMap.keySet());
					for (String key : keySet) {
						if (key.contains(processId)) {
							asyncJobMap.remove(key);
						}
					}

				}
			} catch (Exception e) {
			}

		}
		return resultObj;
	}

	public JSONObject processOnSuccessOrFailExecJob(HttpServletRequest request, JSONObject flowchartData,
			JSONObject operator, String jobId, String resultFlag) {
		JSONObject resultObj = new JSONObject();
		Connection currConnection = null;
		try {
			JSONObject operators = (JSONObject) flowchartData.get("operators");
			if (resultFlag.equalsIgnoreCase("Success") && operator.get("onSuccessExecuteOp") != null) {
				String onSuccessExecuteOpId = String.valueOf(operator.get("onSuccessExecuteOp"));
				JSONObject onSuccessExecOperator = (JSONObject) operators.get(onSuccessExecuteOpId);
				String successJobId = String.valueOf(onSuccessExecOperator.get("jobId"));
				String successJobDesc = String.valueOf(onSuccessExecOperator.get("jobDesc"));
				JSONObject onSuccessOpFlowChartData = processJobComponentsDAO.getJobTransformationRules(request,
						successJobId);
				JSONObject jobResultObj = processJobComponents(request, onSuccessOpFlowChartData, successJobId);
				currConnection = componentUtilities.getCurrentConnection();
				try {
					if (currConnection != null) {
						currConnection.close();
					}
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							successJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				String resultflag = String.valueOf(jobResultObj.get("resultFlag"));
				processOnSuccessOrFailExecJob(request, flowchartData, operator, successJobId, resultflag);
			} else if (resultFlag.equalsIgnoreCase("Fail") && operator.get("onFailExecuteOp") != null) {
				String onFailExecuteOpId = String.valueOf(operator.get("onFailExecuteOp"));
				JSONObject onFailExecOperator = (JSONObject) operators.get(onFailExecuteOpId);
				String failJobId = String.valueOf(onFailExecOperator.get("jobId"));
				String failJobDesc = String.valueOf(onFailExecOperator.get("jobDesc"));
				JSONObject onFailOpFlowChartData = processJobComponentsDAO.getJobTransformationRules(request,
						failJobId);
				JSONObject jobResultObj = processJobComponents(request, onFailOpFlowChartData, failJobId);
				currConnection = componentUtilities.getCurrentConnection();
				try {
					if (currConnection != null) {
						currConnection.close();
					}
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							failJobDesc + " Job Execution Complete", "INFO", 20, "Y", jobId);
				} catch (Exception ex) {
					ex.printStackTrace();
				}

				String resultflag = String.valueOf(jobResultObj.get("resultFlag"));
				processOnSuccessOrFailExecJob(request, flowchartData, operator, failJobId, resultflag);
			}

		} catch (Exception e) {
		} finally {
			try {
				if (currConnection != null) {
					currConnection.close();
				}
			} catch (Exception e) {
			}
		}
		return resultObj;
	}

	// public void cancellProcessJob(String loginUserName, String loginOrgnId,
	// String jobId) {
//        String processId = jobId + "--" + loginUserName + "--" + loginOrgnId;
//        try {
//            List<String> killedJobKeys = new ArrayList();
//            Map<String, Thread> cancelJobsMap = new HashMap<>(asyncJobMap);
//            if (cancelJobsMap != null
//                    && !cancelJobsMap.isEmpty()) {
//                boolean killed = false;
//                for (String key : cancelJobsMap.keySet()) {
//                    if (key.startsWith(processId)) {
//                        Thread currentProcessThread = cancelJobsMap.get(key);
//                        if (currentProcessThread != null && currentProcessThread.isAlive()) {
//                            String threadName = currentProcessThread.getName();
////                    currentProcessThread.interrupt();
//                            try {
//                                if (!currentProcessThread.equals(Thread.currentThread())) {
//                                    currentProcessThread.stop();
////                                    currentProcessThread.interrupt();
//                                    System.out.println(threadName + " +++++++++>>> Thread Killed succesfully");
//                                    killed = true;
//                                }
//
//                            } catch (Exception e) {
//
//                                System.out.println(threadName + " +++++++++>>> Unable to stop thread ");
//                                e.printStackTrace();
//                            }
//
//                        }
//                        killedJobKeys.add(key);
//                    }
//                }
//                if (!killedJobKeys.isEmpty()) {
//                    for (String key : killedJobKeys) {
//                        if (asyncJobMap.containsKey(key)) {
//                            asyncJobMap.remove(key);
//                        }
//
//                    }
//                }
//                if (killed) {
//                    try {
//                        componentUtilities.processETLLog(loginUserName,
//                                loginOrgnId, "ETL process is aborted by user", "ERROR", 10, "N", jobId);
//                    } catch (Exception e) {
//                    }
//                }
//
//            } else {
//                System.out.println("==========>>> Unable to kill job due to empty map");
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            try {
//                componentUtilities.processETLLog(loginUserName,
//                        loginOrgnId, "ETL process is unable to abort due to " + e.getMessage(), "ERROR", 10, "N", jobId);
//            } catch (Exception ex) {
//            }
//        } finally {
//            try {
////                asyncJobMap.remove(processId);
//
//            } catch (Exception e) {
//            }
//
//        }
//    }
	public void cancellAllRunningJobs(String loginUserName, String loginOrgnId, String jobId) {
//        String processId = jobId + "--" + loginUserName + "--" + loginOrgnId;
		try {

			Map<String, ExecutorService> cancelJobsMap = new HashMap<>(asyncJobMap);
			if (cancelJobsMap != null && !cancelJobsMap.isEmpty()) {
				for (String processId : cancelJobsMap.keySet()) {
					if (processId.endsWith("--" + loginUserName + "--" + loginOrgnId)) {
						ExecutorService executor = cancelJobsMap.get(processId);
						if (executor != null) {
							try {
//                                executor.shutdown();
								executor.shutdownNow();
								System.out.println(executor + " +++++++++>>> Shutdown Sucessfully ");
								asyncJobMap.remove(processId);
								try {
									componentUtilities.processETLLog(loginUserName, loginOrgnId,
											"ETL process is aborted by user", "ERROR", 10, "N", jobId);
								} catch (Exception e) {
								}
//                                try {
//                                    if (!executor.awaitTermination(3500, TimeUnit.MILLISECONDS)) {
//                                        executor.shutdownNow();
//                                        if (executor.isTerminated()) {
//                                            System.out.println(executor + " +++++++++>>> Shutdown Sucessfully ");
//                                            asyncJobMap.remove(processId);
//                                            try {
//                                                componentUtilities.processETLLog(loginUserName,
//                                                        loginOrgnId, "ETL process is aborted by user", "ERROR", 10, "N", jobId);
//                                            } catch (Exception e) {
//                                            }
//                                        } else {
//                                            System.out.println(executor + " +++++++++>>> Shutdown unsucessful ");
//                                            try {
//                                                componentUtilities.processETLLog(loginUserName,
//                                                        loginOrgnId, "ETL process not aborted. Trty again", "ERROR", 10, "Y", jobId);
//                                            } catch (Exception e) {
//                                            }
//                                        }
//                                    }
//                                } catch (InterruptedException e) {
//                                    executor.shutdownNow();
//                                }

//                                System.out.println(executor + " +++++++++>>> Shutdown Sucessfully ");
//                                asyncJobMap.remove(processId);
							} catch (Exception e) {
								try {
									componentUtilities.processETLLog(loginUserName, loginOrgnId,
											"ETL process is unable to abort due to " + e.getMessage(), "ERROR", 10, "N",
											jobId);
								} catch (Exception ex) {
								}
							}
							try {
								componentUtilities.processETLLog(loginUserName, loginOrgnId,
										"ETL process is aborted by user", "ERROR", 10, "N", jobId);
							} catch (Exception e) {
							}

						}
					}

				}

			} else {
				System.out.println("==========>>> Unable to kill job due to empty map");
			}

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			try {
//                asyncJobMap.remove(processId);

			} catch (Exception e) {
			}

		}
	}

	public void cancellProcessJob(String loginUserName, String loginOrgnId, String jobId) {
		String processId = jobId + "--" + loginUserName + "--" + loginOrgnId;
		try {

			Map<String, ExecutorService> cancelJobsMap = new HashMap<>(asyncJobMap);
			if (cancelJobsMap != null && !cancelJobsMap.isEmpty()) {

				ExecutorService executor = cancelJobsMap.get(processId);
				if (executor != null) {

					executor.shutdownNow();
					System.out.println(executor + " +++++++++>>> Shutdown Sucessfully ");
					asyncJobMap.remove(processId);
					try {
						componentUtilities.processETLLog(loginUserName, loginOrgnId, "ETL process is aborted by user",
								"ERROR", 10, "N", jobId);
					} catch (Exception e) {
					}
//                    executor.shutdown();
//                                try {
//                                    if (!executor.awaitTermination(3500, TimeUnit.MILLISECONDS)) {
//                                        executor.shutdownNow();
//                                        if (executor.isTerminated()) {
//                                            System.out.println(executor + " +++++++++>>> Shutdown Sucessfully ");
//                                            asyncJobMap.remove(processId);
//                                            try {
//                                                componentUtilities.processETLLog(loginUserName,
//                                                        loginOrgnId, "ETL process is aborted by user", "ERROR", 10, "N", jobId);
//                                            } catch (Exception e) {
//                                            }
//                                        } else {
//                                            System.out.println(executor + " +++++++++>>> Shutdown unsucessful ");
//                                            try {
//                                                componentUtilities.processETLLog(loginUserName,
//                                                        loginOrgnId, "ETL process not aborted. Trty again", "ERROR", 10, "Y", jobId);
//                                            } catch (Exception e) {
//                                            }
//                                        }
//                                    }
//                                } catch (InterruptedException e) {
//                                    executor.shutdownNow();
//                                }

				}

			} else {
				System.out.println("==========>>> Unable to kill job due to empty map");
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				componentUtilities.processETLLog(loginUserName, loginOrgnId,
						"ETL process is unable to abort due to " + e.getMessage(), "ERROR", 10, "N", jobId);
			} catch (Exception ex) {
			}
		} finally {
			try {
//                asyncJobMap.remove(processId);

			} catch (Exception e) {
			}

		}
	}

	public JSONArray getConnectedFromOperatorIds(HttpServletRequest request, String operatorId,
			JSONObject flowchartData) {
		JSONArray connectedFrom = new JSONArray();
		try {

			JSONObject linksData = (JSONObject) flowchartData.get("links");
			for (Object key : linksData.keySet()) {
				JSONObject linkData = (JSONObject) linksData.get(key);
				String toOp = String.valueOf(linkData.get("toOperator"));
				if (toOp.equalsIgnoreCase(operatorId)) {
					connectedFrom.add(String.valueOf(linkData.get("fromOperator")));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connectedFrom;
	}

	public JSONArray getConnectedToOperatorIds(HttpServletRequest request, String operatorId,
			JSONObject flowchartData) {
		JSONArray connectedto = new JSONArray();
		try {

			JSONObject linksData = (JSONObject) flowchartData.get("links");
			for (Object key : linksData.keySet()) {
				JSONObject linkData = (JSONObject) linksData.get(key);
				String fromOp = String.valueOf(linkData.get("fromOperator"));
				if (fromOp.equalsIgnoreCase(operatorId)) {
					connectedto.add(String.valueOf(linkData.get("toOperator")));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connectedto;
	}

	public Map processAPIComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {
		Connection fromConnection = null;
		Object toConnection = null;
		Connection currentConnection = null;
		PreparedStatement preparedStatement = null;
		JSONObject resultObj = new JSONObject();
		int insertCount = 0;
		int count = 0;
		try {

			List fromColumnsList = new ArrayList();
			List fromAliasColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
			List fromDataList = new ArrayList();

			JSONObject operatorData = (JSONObject) operators.get(operatorId);

			JSONObject trfmRules = (JSONObject) operatorData.get("trfmRules");
			String tableStr = "";
			String apiUrl = "";
			String relativePath = "";
			String httpMethod = "";
			String acceptType = "";
			JSONObject parametersObj = new JSONObject();
			if (trfmRules != null) {
				apiUrl = (String) trfmRules.get("apiUrl");
				relativePath = (String) trfmRules.get("relativePath");
				httpMethod = (String) trfmRules.get("httpMethod");
				acceptType = (String) trfmRules.get("acceptType");
				parametersObj = (JSONObject) trfmRules.get("parametersObj");
			}

			// String apiEndpoint =
			// "https://api.nytimes.com/svc/books/v3/lists.json?list=hardcover-fiction&api-key=PAG4AsGEeQSaCDN5pDBorQSFfzZCHebb";
			String apiEndpoint = apiUrl + "/" + relativePath;
			if (parametersObj != null && !parametersObj.isEmpty()) {

				apiEndpoint += "?";
				int i = 0;
				for (Object key : parametersObj.keySet()) {
					Object value = parametersObj.get(key);
					apiEndpoint += key + "=" + value;
					i++;
					if (i != parametersObj.size()) {
						apiEndpoint += "&";
					}

				}
			}
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap();
//             inputMap.add("list", "hardcover-fiction");
//             inputMap.add("api-key", "PAG4AsGEeQSaCDN5pDBorQSFfzZCHebb");

			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<>(inputMap, headers);

			RestTemplate restTemplate = new RestTemplate();
			ResponseEntity<Object> apiResponse = restTemplate.exchange(apiEndpoint, HttpMethod.GET, entity,
					Object.class);
			Object responseBody = apiResponse.getBody();
			ArrayList<LinkedHashMap> responseList;

			if (responseBody instanceof LinkedHashMap) {
			    // If response is a LinkedHashMap, retrieve the "results" key
			    LinkedHashMap responseMap = (LinkedHashMap) responseBody;
			    responseList = (ArrayList<LinkedHashMap>) responseMap.get("results");
			} else if (responseBody instanceof ArrayList) {
			    // If response is already an ArrayList, cast it directly
			    responseList = (ArrayList<LinkedHashMap>) responseBody;
			} else {
			    // Handle unexpected response format
			    throw new IllegalStateException("Unexpected response format: " + responseBody.getClass().getName());
			}
			List<Object[]> dataList = new ArrayList<>();
			Set<String> columnSet = new LinkedHashSet<>();

			// Collect all unique keys for columnsList
			for (LinkedHashMap<String, Object> rowMap : responseList) {
			    columnSet.addAll(rowMap.keySet());
			}

			// Convert the columnSet to a list to preserve order
			List<String> columnsList = new ArrayList<>(columnSet);

			// Create dataList with consistent columns
			for (LinkedHashMap<String, Object> rowMap : responseList) {
			    Object[] rowData = new Object[columnsList.size()];
			    
			    for (int i = 0; i < columnsList.size(); i++) {
			        String key = columnsList.get(i);
			        rowData[i] = rowMap.getOrDefault(key, null); // Fill with null if the key is missing
			    }
			    
			    dataList.add(rowData);
			}

			fromDataList = dataList;

			// String responseStatus = String.valueOf(responseObj.get("status"));
			// resultObject.put("simpleColumnsList", columnsList);
			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			System.out.println(toTableName);
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);
			}

			JSONObject selectedOperator = (JSONObject) operators.get(operatorId);
			JSONObject fromConnObj = (JSONObject) selectedOperator.get("connObj");

			List dataTypeList = (List) columnsList.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());

			fromColumnsList = columnsList;
			toColumnsList = columnsList;
			componentUtilities.createTable(toTableName, columnsList, dataTypeList, (Connection) toConnection);

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {

				String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
				preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

				long startInsertTime = System.currentTimeMillis();
				JSONObject infoObject = new JSONObject();
				// infoObject.put("skipRejectedRecords", skipRejectedRecords);
				infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
						toColumnsList, fromDataList, jobId, infoObject);

				insertCount = (int) infoObject.get("insertCount");
				System.out.println("Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");

				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Processed " + insertCount + " Records Into " + toTableName + " ", "INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}

			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {

				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (fromConnection != null) {
					fromConnection.close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
				if (currentConnection != null) {
					currentConnection.close();
				}
			} catch (Exception e) {
			}
		}
		return operators;
	}

	public JSONObject processNonComponentDebugJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject debugObject = new JSONObject();

		JSONObject resultObj = new JSONObject();
		Object fromConnection = null;
		Object toConnection = null;
		JCO.Client fromJCOConnection = null;
		JCO.Client toJCOConnection = null;
		int insertCount = 0;
		try {

			String offset = request.getParameter("offset");
			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			if (toTableName != null && toTableName.contains(".")) {
				toTableName = toTableName.split("\\.")[1];
			}

			String subJobId = (String) tooperator.get("subJobId");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = componentUtilities.getConnection(toConnObj);

			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			String fromDBType = String.valueOf(fromConnObj.get("CONN_CUST_COL1"));
			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);

				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRulesDataMap = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();
//            Set fromColumnsSet = new LinkedHashSet();
//            Set toColumnsSet = new LinkedHashSet();

			String fromTable = (String) fromOperator.get("tableName");
			String OriginalTableName = (String) fromOperator.get("tableName");

			String skipRejectedRecords = "";
			if (trfmRulesDataMap != null && !trfmRulesDataMap.isEmpty()) {

				skipRejectedRecords = (String) trfmRulesDataMap.get("skipRejectedRecords");
				JSONArray colMappingsData = (JSONArray) trfmRulesDataMap.get("colMappingsData");

				for (int i = 0; i < colMappingsData.size(); i++) {
					JSONObject rowData = (JSONObject) colMappingsData.get(i);
					String destinationColumn = (String) rowData.get("destinationColumn");
					if (destinationColumn.contains("/")) {
						if (destinationColumn.contains(":")) {
							destinationColumn = destinationColumn.split(":")[0] + ":" + "\""
									+ destinationColumn.split(":")[1] + "\"";
						} else {
							destinationColumn = "\"" + destinationColumn.split(":")[1] + "\"";
						}
					}

					if (destinationColumn != null && !"null".equalsIgnoreCase(destinationColumn)
							&& destinationColumn.contains(":")) {
						destinationColumn = destinationColumn.split(":")[1];
					}

					String sourceColumn = (String) rowData.get("sourceColumnActualValue");
					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)
							&& !"null".equalsIgnoreCase(sourceColumn)) {

						if (sourceColumn.contains("/")) {
							if (sourceColumn.contains(":")) {
								sourceColumn = sourceColumn.split(":")[0] + ":" + "\"" + sourceColumn.split(":")[1]
										+ "\"";
							} else {
								sourceColumn = "\"" + sourceColumn.split(":")[1] + "\"";
							}
						}

						if (sourceColumn.contains(":")) {
//                            sourceColumn = sourceColumn.replaceAll(":", ".");
							sourceColumn = sourceColumn.split(":")[1];
						}
						fromColumnsList.add(sourceColumn);
					}
					String sourceTableStr = (String) rowData.get("sourceTable");

					String defaultValue = (String) rowData.get("defaultValue");
//                    String columnClause = (String) rowData.get("columnClause");
					String columnClause = (String) rowData.get("columnClauseActualValue");

					if (sourceColumn != null && !"".equalsIgnoreCase(sourceColumn)
							&& !"null".equalsIgnoreCase(sourceColumn)) {
						JSONArray sourceTableArr = (JSONArray) JSONValue.parse(sourceTableStr);
						String sourceTable = "";
						if (sourceTableArr != null && !sourceTableArr.isEmpty()) {
							sourceTable = (String) sourceTableArr.get(0);
						}
//                        sourceColumn = sourceColumn.replaceAll(":", ".");

					} else if (defaultValue != null && !"".equalsIgnoreCase(defaultValue)) {
						sourceColumn = "'" + defaultValue + "'";
						fromColumnsList.add(sourceColumn);
					} else if (columnClause != null && !"".equalsIgnoreCase(columnClause)) {
						String funcolumnslistStr = (String) rowData.get("funcolumnslist");
						JSONArray funcolumnslist = (JSONArray) JSONValue.parse(funcolumnslistStr);
						sourceColumn = columnClause;
						sourceColumn = sourceColumn.replaceAll(":", ".");
//                        fromColumnsList.addAll(funcolumnslist);
						fromColumnsList.add(sourceColumn);
					}

					toColumnsList.add(destinationColumn);
				}
			}

			List fromColumnsAliasList = new ArrayList();
//            fromColumnsList = new ArrayList(fromColumnsSet);
//            toColumnsList = new ArrayList(toColumnsSet);

			String columnsListStr = "";
			for (int i = 0; i < fromColumnsList.size(); i++) {
				String fromColumn = (String) fromColumnsList.get(i);
				String toColumn = (String) toColumnsList.get(i);
//				if (fromColumn.contains("/")) {
//					fromColumn = "\"" + fromColumn + "\"";
//				}
//				if (toColumn.contains("/")) {
//					toColumn = toColumn.replaceAll("/", "_");
//					if (toColumn.startsWith("_")) {
//						toColumn = toColumn.substring(1);
//					}
//				}
				if (i < (fromColumnsList.size() - 1)) {

					columnsListStr += fromColumn + " AS " + toColumn + ", ";
				} else {
					columnsListStr += fromColumn + " AS " + toColumn + " ";
				}
				fromColumnsAliasList.add(toColumn);
			}

			String query = "SELECT " + columnsListStr + " FROM " + fromTable + " OFFSET " + offset + " ROWS "
					+ "FETCH NEXT 1 ROWS ONLY";
			System.out.println("query :: " + query);

//			PreparedStatement preparedStatement1 = ((Connection) fromConnection).prepareStatement(query);
//			ResultSet resultSet = preparedStatement1.executeQuery();
//			while (resultSet.next()) {
//				Object [] rowData =  new Object[fromColumnsList.size()];
//				for (int i=0; i<fromColumnsList.size(); i++ ) {
//					Object  value = resultSet.getObject(i);
//					rowData[i] = value;
//				}
//				fromDataList.add(rowData);
//				
//			}
			fromDataList = processJobComponentsDAO.getTableDataWithQueryNoLimit(request, query, fromConnObj, jobId);

			if (fromDataList != null && !fromDataList.isEmpty()) {
				Object[] rowDataArray = (Object[]) fromDataList.get(0);
				JSONObject sourceDataObj = new JSONObject();
				for (int i = 0; i < fromColumnsList.size(); i++) {
					String column = (String) fromColumnsList.get(i);
					Object value = rowDataArray[i];
					sourceDataObj.put(column, value);
				}

				debugObject.put("sourceDataObj", sourceDataObj);

			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(toConnObj.get("CONN_CUST_COL1")))) {
					// sap code

					JSONObject sapFromColsObj = (JSONObject) componentUtilities.getColumnsObjFromQuery(query,
							(Connection) fromConnection, fromConnObj);
					List sapFromColsList = (List) sapFromColsObj.get("columnsList");
					List sapFromColsDataTypes = (List) sapFromColsObj.get("dataTypesList");
					if (toConnObj != null && !toConnObj.isEmpty()) {
						toConnection = componentUtilities.getConnection(toConnObj);

					}
					insertCount = erpProcessJobService.insertIntoSapTable(request, toTableName, fromDataList,
							(JCO.Client) toConnection, toColumnsList, sapFromColsDataTypes, jobId);
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");

				} else {
					// db

					String insertQuery = generateInsertQuery(toTableName, toColumnsList);
					if (toConnObj != null && !toConnObj.isEmpty()) {
						toConnection = componentUtilities.getConnection(toConnObj);
					}

					List columnsObj = componentUtilities.getTableColumnsOpt((Connection) toConnection, toConnObj,
							toTableName);

					List toColumnsAllDataTypesList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[3])
							.collect(Collectors.toList());
					Map columnNameDatatypeMap = new HashedMap();
					columnsObj.stream().forEach(
							rowData -> columnNameDatatypeMap.put(((Object[]) rowData)[2], ((Object[]) rowData)[3]));

					List toColumnsDataTypesList = (List) toColumnsList.stream()
							.map(column -> columnNameDatatypeMap.get(column)).collect(Collectors.toList());

					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					infoObject.put("toConnObj", toConnObj);
					infoObject.put("insertQuery", insertQuery);
					infoObject.put("skipRejectedRecords", skipRejectedRecords);
//						infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
//								preparedStatement, toColumnsList, fromDataList, jobId, infoObject);

					infoObject = processJobComponentsDAO.insertDataIntoTableSingleRow(request, toTableName,
							preparedStatement, toColumnsList, fromDataList, toColumnsList, toColumnsDataTypesList,
							fromDBType, jobId, infoObject);

					insertCount = (int) infoObject.get("insertCount");

					JSONObject targetDataObj = (JSONObject) infoObject.get("targetDataObj");

					debugObject.put("targetDataObj", targetDataObj);

					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					if (toTableName.contains("ZZ_TEMP_TABLE")) {
						String abc = "";
					}

					if (skipRejectedRecords != null && "Y".equalsIgnoreCase(skipRejectedRecords)
							&& infoObject.get("rowByRowInsertCount") != null) {
						try {
							componentUtilities.processETLLog(
									(String) request.getSession(false).getAttribute("ssUsername"),
									(String) request.getSession(false).getAttribute("ssOrgId"),
									"Inserted " + infoObject.get("rowByRowInsertCount") + " Records into " + toTableName
											+ " in " + ((jobEndTime - jobStartTime) / 1000) + " Sec",
									"INFO", 20, "Y", jobId);
						} catch (Exception ex) {
						}

					}

				}

			} else {
				// files

				String iconType = String.valueOf(tooperator.get("iconType"));
				String orginalName = (String) tooperator.get("userFileName");
				String filename = "";
				String message = "";
				String userName = (String) request.getSession(false).getAttribute("ssUsername");
				String filePath = etlFilePath + "ETL_EXPORT_" + File.separator + userName;

				String fileExt = String.valueOf(tooperator.get("iconType")).toLowerCase();
				String fileName = "V10ETLExport_" + tooperator.get("timeStamp") + "." + fileExt;
				filename = fileName;
				if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& ("XLSX".equalsIgnoreCase(iconType) || "XLS".equalsIgnoreCase(iconType))) {
					insertCount = fileProcessJobService.insertIntoXLSXFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& ("CSV".equalsIgnoreCase(iconType) || "TXT".equalsIgnoreCase(iconType))) {
					insertCount = fileProcessJobService.insertIntoTextOrCSVFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType) && "XML".equalsIgnoreCase(iconType)) {
					insertCount = fileProcessJobService.insertIntoXMLFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				} else if (iconType != null && !"null".equalsIgnoreCase(iconType)
						&& "JSON".equalsIgnoreCase(iconType)) {
					insertCount = fileProcessJobService.insertIntoJSONFile(request, tooperator, toColumnsList,
							fromDataList, filePath, fileName);
				}

				long jobEndTime = System.currentTimeMillis();
				long jobStartTime = (long) request.getAttribute("jobStartTime");

				message += " <br> <a href='#' style='color:#0071c5;' onclick=downloadExportedFile('" + filename + "','"
						+ orginalName + "') >Click here to download the " + iconType + " file</a>.";//

				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"),
							"Inserted " + insertCount + " Records in " + ((jobEndTime - jobStartTime) / 1000) + " Sec",
							"INFO", 20, "Y", jobId);
				} catch (Exception ex) {
				}

				try {
					componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
							(String) request.getSession(false).getAttribute("ssOrgId"), message, "INFO", 20, "Y",
							jobId);
				} catch (Exception ex) {
				}

			}

			try {
				componentUtilities.processETLJobPreview((String) request.getSession(false).getAttribute("ssUsername"),
						(String) request.getSession(false).getAttribute("ssOrgId"), jobId,
						String.valueOf(tooperator.get("operatorId")), String.valueOf(insertCount));
			} catch (Exception ex) {
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return debugObject;
	}

	public JSONObject processFilterComponentDebugJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		JSONObject debugObject = new JSONObject();

		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			String fromDBType = String.valueOf(fromConnObj.get("CONN_CUST_COL1"));

			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");
			String originalTableName = fromOperator.get("originalTableName") != null
					? (String) fromOperator.get("originalTableName")
					: null;

			String whereClauseCond = "";
			JSONArray whereClauseData = (JSONArray) trfmRules.get("whereClauseData");
			for (int i = 0; i < whereClauseData.size(); i++) {
				String mappedDataStr = (String) whereClauseData.get(i);
				JSONObject mappedData = (JSONObject) JSONValue.parse(mappedDataStr);
				int index = 0;
				for (Object key : mappedData.keySet()) {
					index++;
					JSONObject filterInfo = (JSONObject) mappedData.get(key);
					String columnName = (String) filterInfo.get("columnNameActualValue");

					if (columnName.contains(":")) {
						columnName = columnName.split(":")[1];
					}
					if (columnName.contains(".")) {
						columnName = columnName.split("\\.")[1];
					}

					String operator = (String) filterInfo.get("operator");
					String staticValue = (String) filterInfo.get("staticValue");
					String andOrOperator = (String) filterInfo.get("andOrOperator");
					if (index == mappedData.size()) {
						whereClauseCond += columnName + " " + operator + " '" + staticValue + "' ";
					} else {
						whereClauseCond += columnName + " " + operator + " '" + staticValue + "' " + andOrOperator
								+ " ";
					}
				}
			}
			String offset = request.getParameter("offset");
			String query = "SELECT * FROM " + fromTable + " WHERE " + whereClauseCond + " OFFSET " + offset + " ROWS "
					+ "FETCH NEXT 1 ROWS ONLY";
			;
			System.out.println("query :: " + query);
			String colObjquery = "SELECT * FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;
			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

			fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db

				String insertQuery = generateInsertQuery(toTableName, toColumnsList);
				if (toConnObj != null && !toConnObj.isEmpty()) {
					toConnection = (Connection) componentUtilities.getConnection(toConnObj);
				}

				List columnsObj = componentUtilities.getTableColumnsOpt((Connection) toConnection, toConnObj,
						toTableName);

				List toColumnsAllDataTypesList = (List) columnsObj.stream().map(rowData -> ((Object[]) rowData)[3])
						.collect(Collectors.toList());
				Map columnNameDatatypeMap = new HashedMap();
				columnsObj.stream().forEach(
						rowData -> columnNameDatatypeMap.put(((Object[]) rowData)[2], ((Object[]) rowData)[3]));

				List toColumnsDataTypesList = (List) toColumnsList.stream()
						.map(column -> columnNameDatatypeMap.get(column)).collect(Collectors.toList());

				PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

				long startInsertTime = System.currentTimeMillis();
				JSONObject infoObject = new JSONObject();
				infoObject.put("toConnObj", toConnObj);
				infoObject.put("insertQuery", insertQuery);

//					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName,
//							preparedStatement, toColumnsList, fromDataList, jobId, infoObject);
				infoObject = processJobComponentsDAO.insertDataIntoTableSingleRow(request, toTableName,
						preparedStatement, toColumnsList, fromDataList, toColumnsList, toColumnsDataTypesList,
						fromDBType, jobId, infoObject);

				insertCount = (int) infoObject.get("insertCount");

				JSONObject filterDataObj = (JSONObject) infoObject.get("targetDataObj");

				debugObject.put("filterDataObj", filterDataObj);

			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return debugObject;

	}
	
	public Map processValidateComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		Connection destConnection = null;
		PreparedStatement preparedStatement = null;
		int insertCount = 0;
		int startRange = 0;
		int endRange = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");
			JSONArray trfmRulesDataArray = (JSONArray) trfmRules.get("colMappingsData");
			String fromTable = (String) fromOperator.get("tableName");
			
			for (int i = 0; i < trfmRulesDataArray.size(); i++) {
				JSONObject trfmRulesdata = (JSONObject) trfmRulesDataArray.get(i);
				String destinationConnObjStr = (String) trfmRulesdata.get("destinationConnObj");
				JSONObject destinationConnObj = (JSONObject) JSONValue.parse(destinationConnObjStr);
				if (destinationConnObj != null && !destinationConnObj.isEmpty()) {
					destConnection = (Connection) componentUtilities.getConnection(destinationConnObj);
				}
				String destinationTableColumn = (String) trfmRulesdata.get("destinationColumn");
				String destinationTable = destinationTableColumn.split(":")[1];
				String validationColCheckQuery = "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME='"
						+ destinationTable + "' AND  COLUMN_NAME='VALIDATIONS'";
				System.out.println("validationColCheckQuery :: " + validationColCheckQuery);
				preparedStatement = destConnection.prepareStatement(validationColCheckQuery);
				ResultSet colList = preparedStatement.executeQuery();
				if (colList.next()) {
					String validationsNullSet = "UPDATE " + destinationTable + " SET VALIDATIONS = NULL";
					int alterCount = 0;
					System.out.println("alterQuery :: " + validationsNullSet);
					preparedStatement = destConnection.prepareStatement(validationsNullSet);
					alterCount = preparedStatement.executeUpdate();
				} else {
					String validationsInsert = "ALTER TABLE " + destinationTable
							+ " ADD VALIDATIONS VARCHAR2(1000 CHAR)";
					int alterCount = 0;
					System.out.println("alterQuery :: " + validationsInsert);
					preparedStatement = destConnection.prepareStatement(validationsInsert);
					alterCount = preparedStatement.executeUpdate();
				}
			}

			for (int i = 0; i < trfmRulesDataArray.size(); i++) {
				JSONObject trfmRulesdata = (JSONObject) trfmRulesDataArray.get(i);
				String validationType = (String) trfmRulesdata.get("validationType");
				String parameter = (String) trfmRulesdata.get("parameter");
				String validateColumn = (String) trfmRulesdata.get("sourceColumn");
				String sourceTable = validateColumn.split(":")[0];
				validateColumn = validateColumn.split(":")[1];
				String destinationConnObjStr = (String) trfmRulesdata.get("destinationConnObj");
				JSONObject destinationConnObj = (JSONObject) JSONValue.parse(destinationConnObjStr);
				if (destinationConnObj != null && !destinationConnObj.isEmpty()) {
					destConnection = (Connection) componentUtilities.getConnection(destinationConnObj);
				}
				String destinationTableColumn = (String) trfmRulesdata.get("destinationColumn");
				String destinationTable = destinationTableColumn.split(":")[1];
				String tableColumnValidate = (String) trfmRulesdata.get("tableColumnValidate");
				String validateSchemaTable = "";
				if(tableColumnValidate != null && !"".equalsIgnoreCase(tableColumnValidate)) {
					validateSchemaTable = tableColumnValidate.split(":")[0];
					tableColumnValidate = tableColumnValidate.split(":")[1];
				}
				if (parameter != null && !parameter.isEmpty() && parameter.matches("\\d+-\\d+")) {
					String[] parts = parameter.split("-");
				    startRange = Integer.parseInt(parts[0]);
				    endRange = Integer.parseInt(parts[1]);
				}
//				String validationColCheckQuery = "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME='"
//						+ destinationTable + "' AND  COLUMN_NAME='VALIDATIONS'";
//				System.out.println("validationColCheckQuery :: " + validationColCheckQuery);
//				preparedStatement = destConnection.prepareStatement(validationColCheckQuery);
//				ResultSet colList = preparedStatement.executeQuery();
//				if (colList.next()) {
//					String validationsNullSet = "UPDATE " + destinationTable + " SET VALIDATIONS = NULL";
//					int alterCount = 0;
//					System.out.println("alterQuery :: " + validationsNullSet);
//					preparedStatement = destConnection.prepareStatement(validationsNullSet);
//					alterCount = preparedStatement.executeUpdate();
//				} else {
//					String validationsInsert = "ALTER TABLE " + destinationTable
//							+ " ADD VALIDATIONS VARCHAR2(1000 CHAR)";
//					int alterCount = 0;
//					System.out.println("alterQuery :: " + validationsInsert);
//					preparedStatement = destConnection.prepareStatement(validationsInsert);
//					alterCount = preparedStatement.executeUpdate();
//				}
					int updateCount = 0;
					String validationQuery = "";
					if ("LENGTH".equalsIgnoreCase(validationType)) {
					    validationQuery = "UPDATE " + destinationTable + " SET VALIDATIONS = CASE WHEN VALIDATIONS IS NULL THEN '' ELSE VALIDATIONS || '; ' END"
					        + " || 'Crossed Maximum Length: " + validateColumn + "' WHERE " + validationType + "(" + validateColumn + ") > " + parameter
					        + " AND (VALIDATIONS IS NULL OR VALIDATIONS NOT LIKE '%Crossed Maximum Length: " + validateColumn + "%')";
					} else if ("MANDATORY".equalsIgnoreCase(validationType)) {
					    validationQuery = "UPDATE " + destinationTable + " SET VALIDATIONS = CASE WHEN VALIDATIONS IS NULL THEN '' ELSE VALIDATIONS || '; ' END"
					        + " || 'Mandatory field: " + validateColumn + "' WHERE " + validateColumn + " IS NULL"
					        + " AND (VALIDATIONS IS NULL OR VALIDATIONS NOT LIKE '%Mandatory field: " + validateColumn + "%')";
					} else if ("COLUMNVALIDATE".equalsIgnoreCase(validationType)) {
					    validationQuery = "UPDATE " + destinationTable + " SET VALIDATIONS = CASE WHEN VALIDATIONS IS NULL THEN '' ELSE VALIDATIONS || '; ' END"
					        + " || '" + validateColumn + " Value Missing In Base Table' WHERE " + validateColumn + " NOT IN (SELECT " + tableColumnValidate + " FROM " + validateSchemaTable + ")"
					        + " AND (VALIDATIONS IS NULL OR VALIDATIONS NOT LIKE '%" + validateColumn + " Value Missing In Base Table%')";
					} else if ("UNIQUENESS".equalsIgnoreCase(validationType)) {
						 validationQuery = "UPDATE " + destinationTable + " SET VALIDATIONS = CASE WHEN VALIDATIONS IS NULL THEN '' ELSE VALIDATIONS || '; ' END"
							        + " || 'Duplicate Values: " + validateColumn + "' WHERE " + validateColumn + " IN (SELECT " + validateColumn + " FROM " + sourceTable + " GROUP BY " + validateColumn + " HAVING COUNT(*)>1) "
							        + " AND (VALIDATIONS IS NULL OR VALIDATIONS NOT LIKE '%Duplicate Values: " + validateColumn + "%')";
					} else if ("RANGE".equalsIgnoreCase(validationType)) {
						 validationQuery = "UPDATE " + destinationTable + " SET VALIDATIONS = CASE WHEN VALIDATIONS IS NULL THEN '' ELSE VALIDATIONS || '; ' END"
							        + " || 'Not in Range: " + validateColumn + "' WHERE " + validateColumn + " NOT IN(SELECT " + validateColumn + " FROM " + sourceTable + " WHERE " + validateColumn + ">="+startRange+" AND " + validateColumn + "<="+endRange+")"
							        + " AND (VALIDATIONS IS NULL OR VALIDATIONS NOT LIKE '%Not in Range: " + validateColumn + "%')";
					} else if ("DATATYPE".equalsIgnoreCase(validationType)) {
						 validationQuery = "UPDATE " + destinationTable + " SET VALIDATIONS = CASE WHEN VALIDATIONS IS NULL THEN '' ELSE VALIDATIONS || '; ' END"
							        + " || 'Data Type Mismatch: " + validateColumn + "' WHERE 1<> (SELECT COUNT(*) AS CNT FROM USER_TAB_COLUMNS WHERE DATA_TYPE = '" + parameter + "' AND  TABLE_NAME = '" + sourceTable + "' AND COLUMN_NAME = '" + validateColumn + "')"
							        + " AND (VALIDATIONS IS NULL OR VALIDATIONS NOT LIKE '%Data Type Mismatch: " + validateColumn + "%')";
					}
					System.out.println("validationQuery ::" + validationQuery);
					preparedStatement = destConnection.prepareStatement(validationQuery);
					updateCount = preparedStatement.executeUpdate();

					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Validated " + validateColumn + " From " + fromTable + "", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
			}
			
			for (int i = 0; i < trfmRulesDataArray.size(); i++) {
				JSONObject trfmRulesdata = (JSONObject) trfmRulesDataArray.get(i);
				String destinationConnObjStr = (String) trfmRulesdata.get("destinationConnObj");
				JSONObject destinationConnObj = (JSONObject) JSONValue.parse(destinationConnObjStr);
				if (destinationConnObj != null && !destinationConnObj.isEmpty()) {
					destConnection = (Connection) componentUtilities.getConnection(destinationConnObj);
				}
				String destinationTableColumn = (String) trfmRulesdata.get("destinationColumn");
				String destinationTable = destinationTableColumn.split(":")[1];
				String validationsNullSet = "UPDATE " + destinationTable
						+ " SET VALIDATIONS = 'RECORD VERIFIED' WHERE VALIDATIONS IS NULL";
				int alterCount = 0;
				System.out.println("alterQuery :: " + validationsNullSet);
				preparedStatement = destConnection.prepareStatement(validationsNullSet);
				alterCount = preparedStatement.executeUpdate();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
				if (destConnection != null) {
					((Connection) destConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}
	
	public Map processfetchComponentJob(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {

		JSONObject resultObj = new JSONObject();
		Connection fromConnection = null;
		Connection toConnection = null;
		int insertCount = 0;
		try {

			JSONObject tooperator = (JSONObject) operators.get(operatorId);
			String toTableName = (String) tooperator.get("tableName");
			JSONObject toConnObj = (JSONObject) tooperator.get("connObj");
			if (toConnObj != null && !toConnObj.isEmpty()) {
				toConnection = (Connection) componentUtilities.getConnection(toConnObj);
			}

//            JSONArray fromOperatorList = (JSONArray) tooperator.get("connectedFrom");
			JSONArray fromOperatorList = getConnectedFromOperatorIds(request,
					String.valueOf(tooperator.get("operatorId")), flowchartData);
			Object fromOperatorId = (Object) fromOperatorList.get(0);
			JSONObject fromOperator = (JSONObject) operators.get(String.valueOf(fromOperatorId));
			JSONObject fromConnObj = (JSONObject) fromOperator.get("connObj");

			List fromDataList = new ArrayList();
			if (fromConnObj != null && fromConnObj.containsKey("HOST_NAME")) {
				if ("SAP_ECC".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))
						|| "SAP_HANA".equalsIgnoreCase(String.valueOf(fromConnObj.get("CONN_CUST_COL1")))) {
					// sap code
					fromOperator = sapToLocalTable(request, fromOperator, jobId);
				} else {
					// db
				}
			} else {
				// files
				fromOperator = fileToLocalTable(request, fromOperator, jobId);
			}
			operators.put(String.valueOf(fromOperatorId), fromOperator);
			fromConnObj = (JSONObject) fromOperator.get("connObj");
			if (fromConnObj != null && !fromConnObj.isEmpty()) {
				fromConnection = (Connection) componentUtilities.getConnection(fromConnObj);
			}
			JSONObject trfmRules = (JSONObject) tooperator.get("trfmRules");

			List fromColumnsList = new ArrayList();
			List toColumnsList = new ArrayList();

			String fromTable = (String) fromOperator.get("tableName");
			String originalTableName = fromOperator.get("originalTableName") != null
					? (String) fromOperator.get("originalTableName")
					: null;

			String sourceColsStr = "";
			String query = "";
			JSONArray trfmRulesDataArray = (JSONArray) trfmRules.get("colMappingsData");
			for (int i = 0; i < trfmRulesDataArray.size(); i++) {
			JSONObject trfmRulesdata = (JSONObject) trfmRulesDataArray.get(i);
			String compareTableColumn = (String) trfmRulesdata.get("compareColumn");
			String compareColumn = compareTableColumn.split(":")[1];
			String tableColumnValidate = (String) trfmRulesdata.get("tableColumnValidate");
			String tableColumnValidateTable = tableColumnValidate.split(":")[0];
			String tableValidatecolumn = tableColumnValidate.split(":")[1];
			JSONArray sourceColumns =  (JSONArray)trfmRulesdata.get("sourceColumns");
			sourceColsStr =(String) sourceColumns.stream()
                    .map(e -> ((String) e).startsWith("TEXT") ? "\"" + e + "\"" : e)
                    .collect(Collectors.joining(","));
			String likeQuery = "SELECT "+tableValidatecolumn+" FROM "+tableColumnValidateTable+"";
			query = "SELECT "+sourceColsStr+" FROM "+fromTable+" WHERE ";
			query += "TEXT".equalsIgnoreCase(compareColumn) ? "\"" + compareColumn + "\" " : compareColumn + " ";
			query += "LIKE '%'||("+likeQuery+")||'%'";
			System.out.println("query :: " + query);
			}

			
			//System.out.println("query :: " + query);
			String colObjquery = "SELECT "+sourceColsStr+" FROM " + fromTable + " WHERE 1=2";
			JSONObject columnsObject = componentUtilities.getColumnsObjFromQuery(colObjquery, fromConnection,
					fromConnObj);
			List dataTypesList = (List) columnsObject.get("dataTypesList");
			List columnsList = (List) columnsObject.get("columnsList");
			fromColumnsList = columnsList;
			toColumnsList = columnsList;
			componentUtilities.createTable(toTableName, columnsList, dataTypesList, toConnection);

//			if (!(toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
//					&& fromConnObj.containsKey("HOST_NAME")
//					&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
//					&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
//					&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
//					&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
//					&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1")))) {
//				fromDataList = processJobComponentsDAO.getTableDataWithQuery(request, query, fromConnObj, jobId);
//
//			}

			if (toConnObj != null && toConnObj.containsKey("HOST_NAME")) {
				// db
				if (toConnObj != null && fromConnObj != null && toConnObj.containsKey("HOST_NAME")
						&& fromConnObj.containsKey("HOST_NAME")
						&& toConnObj.get("HOST_NAME").equals(fromConnObj.get("HOST_NAME"))
						&& toConnObj.get("CONN_PORT").equals(fromConnObj.get("CONN_PORT"))
						&& toConnObj.get("CONN_USER_NAME").equals(fromConnObj.get("CONN_USER_NAME"))
						&& toConnObj.get("CONN_PASSWORD").equals(fromConnObj.get("CONN_PASSWORD"))
						&& toConnObj.get("CONN_CUST_COL1").equals(fromConnObj.get("CONN_CUST_COL1"))) {

					insertCount = processJobComponentsDAO.mergeUsingQuery(request, toTableName, fromColumnsList,
							toColumnsList, toConnection, query, jobId);

					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				} else {
					String insertQuery = generateInsertQuery(toTableName, new ArrayList<String>(toColumnsList));
					PreparedStatement preparedStatement = ((Connection) toConnection).prepareStatement(insertQuery);

					long startInsertTime = System.currentTimeMillis();
					JSONObject infoObject = new JSONObject();
					// infoObject.put("skipRejectedRecords", skipRejectedRecords);
					infoObject = processJobComponentsDAO.insertDataIntoTable(request, toTableName, preparedStatement,
							toColumnsList, fromDataList, jobId, infoObject);
					insertCount = (int) infoObject.get("insertCount");
					System.out.println(
							"Insert time  :: " + (System.currentTimeMillis() - startInsertTime) / 1000 + " Sec");
					long jobEndTime = System.currentTimeMillis();
					long jobStartTime = (long) request.getAttribute("jobStartTime");
					try {
						componentUtilities.processETLLog((String) request.getSession(false).getAttribute("ssUsername"),
								(String) request.getSession(false).getAttribute("ssOrgId"),
								"Processed " + insertCount + " Records ", "INFO", 20, "Y", jobId);
					} catch (Exception ex) {
					}
				}
			}

			resultObj.put("insertCount", insertCount);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			try {
				if (fromConnection != null) {
					((Connection) fromConnection).close();
				}
				if (toConnection != null) {
					((Connection) toConnection).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return operators;

	}
	
	public Map processFileFolderComponent(HttpServletRequest request, String operatorId, Map operators,
			JSONObject flowchartData, String jobId) {
		try {
			JSONObject tooperator = (JSONObject) operators.get(operatorId);
//			String fileFolderPath = (String) tooperator.get("fileFolderPath");
//			JSONObject files = (JSONObject) tooperator.get("files");
			folderFileToLocalTable(request, tooperator, jobId);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return operators;

	}
	
	public JSONObject folderFileToLocalTable(HttpServletRequest request, JSONObject fromOperator, String jobId) {

		Connection currentConnection = null;
		PreparedStatement preparedStatement = null;
		List fromColumnsList = new ArrayList();
		List dataTypesList = new ArrayList();
		List<String> fromDataList = new ArrayList();
		String fileName = "";
		try {
			List<MultipartFile> multipartfiles = (List<MultipartFile>) request.getAttribute("files");
			for (MultipartFile file : multipartfiles) {
				fileName = file.getOriginalFilename();
				String fileType = fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length());
				fileType = fileType.toUpperCase();
				String filePath = fileProcessJobService.folderFileDMImport(request, file, fileName);
				fromColumnsList = componentUtilities.getHeadersOfImportedFile(request, filePath);
				fromColumnsList = componentUtilities.fileHeaderValidations(fromColumnsList);
//	            List dataTypesList = componentUtilities.getHeaderDataTypesOfImportedFile(request, filePath);
//	            List dataTypesList = (List) fromColumnsList.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());
				JSONObject dataTypesObj = (JSONObject) fromOperator.get("dataTypesObj");
				
				if (dataTypesObj != null) {
					for (Object col : fromColumnsList) {
						JSONObject typeObj = (JSONObject) dataTypesObj.get(col);
						String datatype = (String) typeObj.get("datatype");
						String columnsize = String.valueOf(typeObj.get("columnsize"));
						if (columnsize != null && !"".equalsIgnoreCase(columnsize)) {
							datatype = datatype + "(" + columnsize + ")";
						}
						dataTypesList.add(datatype);
					}
				} else {
//	                dataTypesList = (List) fromColumnsList.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());
					dataTypesList = componentUtilities.getHeaderDataTypesOfImportedFile(request, filePath);
				}

				String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column)
						.collect(Collectors.joining(","));
				
				if (fileType != null) {
					fileType = fileType.replaceAll("\\.", "");
				}
				if (fileType != null && !"null".equalsIgnoreCase(fileType)
						&& ("XLSX".equalsIgnoreCase(fileType) || "XLS".equalsIgnoreCase(fileType))) {
					fromDataList.addAll(fileProcessJobService.readExcelFile(request, filePath, fileName));
				} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && ("CSV".equalsIgnoreCase(fileType)
						|| "TXT".equalsIgnoreCase(fileType) || "JSON".equalsIgnoreCase(fileType))) {
					fromDataList.addAll(fileProcessJobService.readCSVFile(request, filePath, fileName, fileType,
							fromColumnsList));
				} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "XML".equalsIgnoreCase(fileType)) {
					fromDataList.addAll(fileProcessJobService.readXMLFile(request, filePath, fileName));
				} else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "PDF".equalsIgnoreCase(fileType)) {
					fromDataList.addAll(fileProcessJobService.readPDF(request, filePath, fileName));
				}
			}
//			JSONArray fileFolderPathdetails = (JSONArray) fromOperator.get("fileFolderPathdetails");
//			String fileName = "";
//			for(int i = 0; i<fileFolderPathdetails.size(); i++) {
//				JSONObject fileFolderDetails = (JSONObject)fileFolderPathdetails.get(i);
//				String fileFolderPath = (String) fileFolderDetails.get("fileLocation");
//				String folderFileName = (String) fileFolderDetails.get("fileName");
//				fileName = folderFileName;
//				String filePath = fileFolderPath + "\\" + fileName;
//				String fileType = folderFileName.split("\\.")[1];
//				if(folderFileName.startsWith("*")) {
//					File folder = new File(fileFolderPath);
//					if (folder.exists() && folder.isDirectory()) {
//						File[] files = folder.listFiles();
//						if (files != null) {
//							for (File file : files) {
//									filePath = fileFolderPath + "\\" + file.getName();
//									fileName = file.getName();
//									String mainfilePath = fileProcessJobService.folderFileDMImport(request, filePath, fileName);
//									fromColumnsList = componentUtilities.getHeadersOfImportedFile(request, mainfilePath);
//									fromColumnsList = componentUtilities.fileHeaderValidations(fromColumnsList);
//									JSONObject dataTypesObj = (JSONObject) fromOperator.get("dataTypesObj");
//									if (dataTypesObj != null) {
//										for (Object col : fromColumnsList) {
//											JSONObject typeObj = (JSONObject) dataTypesObj.get(col);
//											String datatype = (String) typeObj.get("datatype");
//											String columnsize = String.valueOf(typeObj.get("columnsize"));
//											if (columnsize != null && !"".equalsIgnoreCase(columnsize)) {
//												datatype = datatype + "(" + columnsize + ")";
//											}
//											dataTypesList.add(datatype);
//										}
//									} else {
////			                        dataTypesList = (List) fromColumnsList.stream().map(e -> "VARCHAR2(4000)").collect(Collectors.toList());
//										dataTypesList = componentUtilities.getHeaderDataTypesOfImportedFile(request,
//												mainfilePath);
//									}
//
//									String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column)
//											.collect(Collectors.joining(","));
//									if (fileType != null) {
//										fileType = fileType.replaceAll("\\.", "");
//									}
//									if (fileType != null && !"null".equalsIgnoreCase(fileType)
//											&& ("XLSX".equalsIgnoreCase(fileType) || "XLS".equalsIgnoreCase(fileType))) {
//										fromDataList
//												.addAll(fileProcessJobService.readExcelFile(request, mainfilePath, fileName));
//									} else if (fileType != null && !"null".equalsIgnoreCase(fileType)
//											&& ("CSV".equalsIgnoreCase(fileType) || "TXT".equalsIgnoreCase(fileType)
//													|| "JSON".equalsIgnoreCase(fileType))) {
//										fromDataList.addAll(fileProcessJobService.readCSVFile(request, mainfilePath, fileName,
//												fileType, fromColumnsList));
//									} else if (fileType != null && !"null".equalsIgnoreCase(fileType)
//											&& "XML".equalsIgnoreCase(fileType)) {
//										fromDataList.addAll(fileProcessJobService.readXMLFile(request, mainfilePath, fileName));
//									} else if (fileType != null && !"null".equalsIgnoreCase(fileType)
//											&& "PDF".equalsIgnoreCase(fileType)) {
//										fromDataList.addAll(fileProcessJobService.readPDF(request, mainfilePath, fileName));
//									}
//								
//							}
//			            }
//					}
//				}
//				else {
					
//				}
//			}
			
			String stagingTableName = "ZZ_TEMP_" + AuditIdGenerator.genRandom32Hex();

			String createTableQuery = "CREATE TABLE " + stagingTableName + "( ";
			for (int i = 0; i < fromColumnsList.size(); i++) {

				String columnName = String.valueOf(fromColumnsList.get(i));
				if (columnName.contains("/")) {
					columnName = "\"" + columnName + "\"";
				}

				if (i < (fromColumnsList.size() - 1)) {
					createTableQuery += columnName + " " + dataTypesList.get(i) + " , ";
				} else {
					createTableQuery += columnName + " " + dataTypesList.get(i) + "";
				}

			}
//            createTableQuery += fromColumnsList.stream().map(column -> column + " VARCHAR2(4000) ").collect(Collectors.joining(","));
			createTableQuery += " )";
			currentConnection = componentUtilities.getCurrentConnection();
			preparedStatement = currentConnection.prepareStatement(createTableQuery);
			preparedStatement.execute();

			((List) request.getAttribute("stagingTablesList")).add(stagingTableName);
			request.setAttribute("stagingTablesList", request.getAttribute("stagingTablesList"));

			String insertQuery = generateInsertQuery(stagingTableName, fromColumnsList);
			preparedStatement = currentConnection.prepareStatement(insertQuery);
			JSONObject infoObject = new JSONObject();
			// infoObject.put("skipRejectedRecords", skipRejectedRecords);
//            int insertCount = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement, fromColumnsList, fromDataList, jobId);
			infoObject = processJobComponentsDAO.insertDataIntoTable(request, stagingTableName, preparedStatement,
					fromColumnsList, fromDataList, fromColumnsList, dataTypesList, "ORACLE", jobId, infoObject);
			int insertCount = (int) infoObject.get("insertCount");
			fromOperator.put("originalTableName", fileName.replaceAll("[^a-zA-Z0-9]", "_"));
			fromOperator.put("tableName", stagingTableName);
			fromOperator.put("statusLabel", stagingTableName);
			fromOperator.put("CONN_CUST_COL1", "DB");
			fromOperator.put("columnsList", fromColumnsList);
			fromOperator.put("connObj",
					new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password, "Current_V10"));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
//            if (sapConnection != null) {
//                sapConnection.close();
//            }
				if (currentConnection != null) {
					currentConnection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fromOperator;
	}
	

}
