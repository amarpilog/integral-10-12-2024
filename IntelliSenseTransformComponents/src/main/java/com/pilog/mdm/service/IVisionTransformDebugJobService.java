package com.pilog.mdm.service;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.map.HashedMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class IVisionTransformDebugJobService {

	@Autowired
	IVisionTransformComponentUtilities componentUtilities;
	@Autowired
	IVisionTransformProcessJobComponentsService processJobComponentsService;
	public JSONObject debugProcessJob(HttpServletRequest request, JSONObject flowchartData, String jobId) {
		
		final int numberOfThreads = 100;
		
		
		java.util.Date jobStartTime = new java.util.Date();
		long jobStartTimeLong = System.currentTimeMillis();
		Connection currConnection = null;
		JSONObject componentsResultObj = new JSONObject();
		JSONObject resultObj = new JSONObject();
		List dropTablesList = new ArrayList();

		request.setAttribute("stagingTablesList", new ArrayList());
		
		try {
		
			request.setAttribute("jobStartTime", System.currentTimeMillis());
//          String flowchartDataStr = request.getParameter("flowchartData");
//          JSONObject flowchartData = (JSONObject) JSONValue.parse(flowchartDataStr);
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

							connectedFromOpArray = processJobComponentsService.getConnectedFromOperatorIds(request,
									String.valueOf(operator.get("operatorId")), flowchartData);

							if (isComponent != null && "Y".equalsIgnoreCase(isComponent)) {

								if (iconType != null && "UNIQUE".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processUniqueComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);
									
							
								} else if (iconType != null && "GROUP".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processDenormalizeComponentJob(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "UNGROUP".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processNormalizeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "FILTER".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
								
									resultObj = processJobComponentsService.processFilterComponentDebugJob(request, (String) operatorId, operators, flowchartData,
											jobId);
									componentsResultObj.putAll(resultObj);
									
								} else if (iconType != null && "OUTPUT".equalsIgnoreCase(iconType)) {
									processJobComponentsService.processOutputComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "STAGING".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processStagingComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "MERGE_FILES".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processMergeFilesComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "JOIN".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processJoinsComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SORT".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processSortComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "GROUPBY".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processGroupByComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "QUERY".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processQueryComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SCD".equalsIgnoreCase(iconType)) {

									JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
									String selectedSCDType = (String) trfmRules.get("selectedSCDType");
									if (selectedSCDType != null && "SCD1".equalsIgnoreCase(selectedSCDType)) {

//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.getSession(false).setAttribute(
												"skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);

										processJobComponentsService.processSCD1ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD2".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.getSession(false).setAttribute(
												"skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);

										processJobComponentsService.processSCD2ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD3".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.getSession(false).setAttribute(
												"skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processJobComponentsService.processSCD3ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);

									} else if (selectedSCDType != null && "SCD4".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.getSession(false).setAttribute(
												"skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processJobComponentsService.processSCD4ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									} else if (selectedSCDType != null && "SCD6".equalsIgnoreCase(selectedSCDType)) {
//                                JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
										JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										request.getSession(false).setAttribute(
												"skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
												skipOperatorIds);
										processJobComponentsService.processSCD6ComponentJob(request, (String) operatorId, operators, flowchartData,
												jobId);
									}
								} else if (iconType != null && "MERGE".equalsIgnoreCase(iconType)) {
//                            JSONArray skipOperatorIds = (JSONArray) operator.get("connectedTo");
									JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.getSession(false).setAttribute(
											"skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);

									JSONObject trfmRules = (JSONObject) operator.get("trfmRules");
									String operatorType = (String) trfmRules.get("trfmRules");
									if (operatorType != null && "Insert".equalsIgnoreCase(operatorType)) {
//                                JSONArray connectedToArray = (JSONArray) operator.get("connectedTo");
										JSONArray connectedToArray = processJobComponentsService.getConnectedToOperatorIds(request,
												String.valueOf(operator.get("operatorId")), flowchartData);
										List rejectedOperatorsList = new ArrayList();
										boolean hasRejectedComponent = operators.values().stream().anyMatch(op -> {
											if ("REJECTED".equalsIgnoreCase(
													String.valueOf(((JSONObject) op).get("iconType")))) {
//                                        JSONArray connectedFromArray = (JSONArray) ((JSONObject) op).get("connectedFrom");
												JSONArray connectedFromArray = (JSONArray) processJobComponentsService.getConnectedToOperatorIds(
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
											JSONArray skipOperators = (JSONArray) request.getSession(false)
													.getAttribute("skipOperators_" + jobId + "_"
															+ operator.get("operatorId"));
											if (skipOperators != null) {
												skipOperators.add(rejectedOperatorId);
											}
//                                            request.setAttribute("skipOperators", skipOperators);
											request.getSession(false).setAttribute(
													"skipOperators_" + jobId + "_" + skipOperatorIds.get(0),
													skipOperatorIds);
											processJobComponentsService.processRejectedComponentJob(request, (String) rejectedOperatorId, operators,
													flowchartData, jobId);

										}
									}

									processJobComponentsService.processMergeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "REJECTED".equalsIgnoreCase(iconType)) {
									processJobComponentsService.processRejectedComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SAPLOAD".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.getSession(false).setAttribute(
											"skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"), skipOperatorIds);
//                                    JSONArray skipOperators11 = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId+ "_" + operator.get("operatorId"));

									processJobComponentsService.processOneToOneComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);
									
								} else if (iconType != null && "SAPLOADREVERSE".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.getSession(false).setAttribute(
											"skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"), skipOperatorIds);
//                                    JSONArray skipOperators11 = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId+ "_" + operator.get("operatorId"));

									processJobComponentsService.processOneToOneComponentJobReverse(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "SAPLOADSTANDARD".equalsIgnoreCase(iconType)) {
									JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.getSession(false).setAttribute(
											"skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"), skipOperatorIds);
//                                    JSONArray skipOperators11 = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId+ "_" + operator.get("operatorId"));

									processJobComponentsService.processOneToOneComponentJobStandard(request, (String) operatorId, operators,
											flowchartData, jobId);

								} else if (iconType != null && "SAPRESUME".equalsIgnoreCase(iconType)) {

									JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.getSession(false).setAttribute(
											"skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
									processJobComponentsService.processSapResumeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "SAPJOIN".equalsIgnoreCase(iconType)) {

									JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request,
											String.valueOf(operator.get("operatorId")), flowchartData);
									request.getSession(false).setAttribute(
											"skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
									processJobComponentsService.processSapJoinComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "PIVOT".equalsIgnoreCase(iconType)) {
//                                    JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")), flowchartData);
//                                    request.setAttribute("skipOperators", skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
									processJobComponentsService.processPivotComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "PIVOT".equalsIgnoreCase(iconType)) {
//                                    JSONArray skipOperatorIds = processJobComponentsService.getConnectedToOperatorIds(request, String.valueOf(operator.get("operatorId")), flowchartData);
//                                    request.setAttribute("skipOperators", skipOperatorIds);
//                                    request.getSession(false).setAttribute("skipOperators_" + jobId + "_" + skipOperatorIds.get(0), skipOperatorIds);
									processJobComponentsService.processUnpivotComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								} else if (iconType != null && "ROWS_RANGE".equalsIgnoreCase(iconType)) {
									String stagingTable = (String) operator.get("tableName");
									((List) request.getAttribute("stagingTablesList")).add(stagingTable);
									request.setAttribute("stagingTablesList",
											request.getAttribute("stagingTablesList"));
									processJobComponentsService.processRowsRangeComponentJob(request, (String) operatorId, operators, flowchartData,
											jobId);

								}

							} else if (!connectedFromOpArray.isEmpty()) {
//                                JSONArray skipOperators = (JSONArray) request.getSession(false).getAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"));
								JSONArray skipOperators = (JSONArray) request.getSession(false)
										.getAttribute("skipOperators_" + jobId + "_" + operator.get("operatorId"));
//                        long operatorIdLong = Long.valueOf(String.valueOf(operatorId));
								if (!(skipOperators != null && skipOperators.contains(operatorId))) {
									request.getSession(false).removeAttribute("skipOperators_" + jobId);
									resultObj = processJobComponentsService.processNonComponentDebugJob(request, (String) operatorId, operators, flowchartData,
											jobId);
									componentsResultObj.putAll(resultObj);
								}
							}
						}

				index++;
				if (index == 1) {
					
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
			} catch (Exception ex) {
			}
			

		}
		return componentsResultObj;
	}

}
