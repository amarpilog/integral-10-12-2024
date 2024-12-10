/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Ravindar.P
 */
package com.pilog.mdm.controller;

import com.pilog.mdm.service.IVisionProcSchedulingService;
import com.pilog.mdm.service.IVisionTransformComponentUtilities;


import com.pilog.mdm.service.IVisionTransformComponentsService;
import com.pilog.mdm.service.V10JobSchedulingProcessService;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class IVisionTransformComponentsController {

    @Autowired
    private IVisionTransformComponentsService iVisionTransformComponentsService;

    @Autowired
    private IVisionTransformComponentUtilities componentUtilities;

    @Autowired
    private V10JobSchedulingProcessService jobSchedulingProcessService;
    
      @Autowired
    private IVisionProcSchedulingService procSchedulingService;
    
   
    
    @Value("${jdbc.driver}")
    private String dataBaseDriver;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String password;
    @Value("${jdbc.url}")
    private String dbURL;

    @Value("${MultipartResolver.fileUploadSize}")
    private long maxFileSize;
    @Value("${MultipartResolver.fileinMemorySize}")
    private int maxMemSize;

    @RequestMapping(value = "/getuniqueRecords", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject getuniqueRecords(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.getuniqueRecords(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    @RequestMapping(value = "/uniqueComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject uniqueComponentTrfmRules(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.uniqueComponentTrfmRules(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

        @RequestMapping(value = "/scdType3TrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject scdType3TrfmRules(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.scdType3TrfmRules(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }
    
            @RequestMapping(value = "/scdType6TrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject scdType6TrfmRules(HttpServletRequest request) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.scdType6TrfmRules(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }
   
    @RequestMapping(value = "/mergeComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject mergeComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.mergeComponentTrfmRules(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
       
        return resultObject;
    }
    
    @RequestMapping(value = "/columnMappingTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject columnMappingTrfmRules(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.columnMappingTrfmRules(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    @RequestMapping(value = "/columnMappingTrfmRulesQueryComp", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject columnMappingTrfmRulesQueryComp(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.columnMappingTrfmRulesQueryComp(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }
    
    @RequestMapping(value = "/columnMappingTrfmRulesAPIComp", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject columnMappingTrfmRulesAPIComp(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.columnMappingTrfmRulesAPIComp(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }
    
    @RequestMapping(value = "/getAPIColumns", method = {RequestMethod.POST, RequestMethod.GET}, produces = "application/json;charset=UTF-8")
    public @ResponseBody JSONObject getAPIColumns(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.getAPIColumns(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }
    
    @RequestMapping(value = "/columnMappingTrfmRulesForComponent", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject columnMappingTrfmRulesForComponent(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.columnMappingTrfmRulesForComponent(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    @RequestMapping(value = "/filterComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject filterComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.filterComponentTrfmRules(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    @RequestMapping(value = "/joinComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject joinComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.joinComponentTrfmRules(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    @RequestMapping(value = "/sortComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject sortComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.sortComponentTrfmRules(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }
    
    @RequestMapping(value = "/rowsRangeComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject rowsRangeComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.rowsRangeComponentTrfmRules(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    @RequestMapping(value = "/groupByComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONObject groupByComponentTrfmRules(HttpServletRequest request, HttpServletResponse response) {
        JSONObject resultObject = new JSONObject();
        try {
            resultObject = iVisionTransformComponentsService.groupByComponentTrfmRules(request, response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObject;
    }

    @RequestMapping(value = "/fetchJoinTablesData", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    JSONObject fetchJoinTablesData(HttpServletRequest request) {

        JSONObject joinTablesData = new JSONObject();
        try {
            joinTablesData = iVisionTransformComponentsService.fetchJoinTableColumnTrfnRules(request);
//            joinTablesData = dataPipingService.fetchJoinTablesData(request);
        } catch (Exception e) {
        }
        return joinTablesData;
    }

    @RequestMapping(value = "/showingClauseColumns", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody
    String showingClauseColumns(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {

            String clauseCondStr = request.getParameter("whereClauseStr");
            JSONObject whereClauseObj = new JSONObject();
            if (clauseCondStr != null && !"".equalsIgnoreCase(clauseCondStr)) {
                whereClauseObj = (JSONObject) JSONValue.parse(clauseCondStr);
            }
            String whereClauseColsCondition = "<div class=\"visionEtlJoinClauseMain\">"
                    + "<div class=\"visionEtlAddIconDiv\">"
                    + "<img data-trstring='' src=\"images/add.png\" id=\"visionEtlAddRowIcon\" "
                    + "class=\"visionEtlAddRowIcon\" title=\"Add new where clause\""
                    + " onclick=addNewClauseRow(event,id,this) "
                    + "style=\"width:18px;height: 18px;cursor:pointer; float: left;\"/>"
                    + "<img data-trstring='' src=\"images/save_icon2.png\" id=\"visionEtlSaveIcon\" "
                    + "class=\"visionEtlAddRowIcon\" title=\"Save Mapping\""
                    + " onclick=saveClauseMapping(event,id,this) "
                    + "style=\"width:18px;height: 18px;cursor:pointer; float: left;\"/>"
                    + "</div>"
                    + "<div class=\"visionEtlJoinClauseTablesDiv visionEtlJoinClauseTablesDivScroll\">"
                    + "<table id=\"fromTablesWhereCauseTable\""
                    + " class=\"visionEtlJoinClauseTable\" style=\"width: 100%;\" border=\"1\">"
                    + "<thead>"
                    + "<tr>"
                    + "<th width='5%' class=\"\" "
                    + "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\"></th>"
                    + "<th width='35%' class=\"\" "
                    + "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
                    + "Column Name"
                    + "</th>"
                    + "<th width='20%' class=\"\""
                    + " style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
                    + "Opeartor"
                    + "</th>"
                    + "<th width='30%' class=\"\" "
                    + "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
                    + "Value"
                    + "</th>"
                    + "<th width='10%' class=\"\" "
                    + "style=\"background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center\">"
                    + "AND/OR"
                    + "</th>"
                    + "</tr>"
                    + "</thead>"
                    + "<tbody>";

            // ravi start
            String trString = "";
            String singletrString = "<tr>"
                    + "<td width='5%'>"
                    + "<img src=\"images/Delete_Red_Icon.svg\" "
                    + "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
                    + "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
                    + "<td width='35%'  class=\"sourceJoinColsTd\">"
                    + "<input class=\"visionColJoinMappingInput visionColFuncInput\" type=\"text\" value=\"\" readonly=\"true\">"
                    + "<img src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
                    + " onclick=\"selectColumn(this,'fromWhereClauseColumn')\" style=\"margin-left: 2px;\">"
                    + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" onclick=\"selectColumnFun(this,'fromWhereClauseColumn')\" style=\"margin-left: 2px;\">"
                    + "</td>"
                    + "<td width='20%' class=\"sourceJoinColsTd\">"
                    + "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\" onchange=\"disableDdw(this,id)\">"
                    + "<option  value='='>=</option>"
                    + "<option  value='!='>!=</option>"
                    + "<option  value='LIKE'>LIKE</option>"
                    + "<option  value='NOT LIKE'>NOT LIKE</option>"
                    + "<option  value='IN'>IN</option>"
                    + "<option  value='NOT IN'>NOT IN</option>"
                    + "<option  value='<'><</option>"
                    + "<option  value='>'>></option>"
                    + "<option  value='<='><=</option>"
                    + "<option  value='>='>>=</option>"
                    + "<option  value='IS'>IS</option>"
                    + "<option  value='IS NOT'>IS NOT</option>"
                    + "</select>"
                    + "</td>"
                    + "<td width='30%' class=\"sourceJoinColsTd\">"
                    + "<input class='visionColMappingTextarea' type=\"text\" value=\"\">"
                    + "</td>"
                    + "<td width='10%' class=\"sourceJoinColsTd\">"
                    + "<select id='andOrOpt'>"
                    + "<option value='AND'>AND</option>"
                    + "<option value='OR'>OR</option>"
                    + "</select>"
                    + "</td>"
                    + "</tr>";
            String trfmRulesId = request.getParameter("trfmRulesId");
            String trfmRulesChanged = request.getParameter("trfmRulesChanged");

            trString += singletrString;
            // ravi end
            String whereClauseTrString = "";
            if (whereClauseObj != null && !whereClauseObj.isEmpty()) {
                int row = 1;
                for (Object keyName : whereClauseObj.keySet()) {
                    JSONObject dataClauseObj = (JSONObject) whereClauseObj.get(keyName);
                    if (dataClauseObj != null && !dataClauseObj.isEmpty()) {
                        String operator = (String) dataClauseObj.get("operator");
                        whereClauseTrString += "<tr>"
                                + "<td width='5%'>"
                                + "<img src=\"images/Delete_Red_Icon.svg\" "
                                + "onclick=\"deleteSelectedRow(this)\" class=\"visionColMappingImg\" "
                                + "title=\"Delete\" style=\"width:15px;height: 15px;cursor:pointer;\"></td>"
                                + "<td width='35%'  class=\"sourceJoinColsTd\">"
                                + "<input class=\"visionColJoinMappingInput visionColFuncInput\" type=\"text\""
                                + " value=\"" + dataClauseObj.get("columnName") + "\"  "
                                + " actual-value=\"" + dataClauseObj.get("columnNameActualValue") + "\"  "
                                + " funcolumnslist=\"" + String.valueOf(dataClauseObj.get("funcolumnslist")).replaceAll("'", "&#39;") + "\"  "
                                + "data-funobjstr='" + String.valueOf(dataClauseObj.get("datafunobjstr")).replaceAll("'", "&#39;") + "'"
                                + " readonly=\"true\">"
                                + "<img src=\"images/tree_icon.svg\" class=\"visionETLColMapImage \""
                                + " onclick=\"selectColumn(this,'fromWhereClauseColumn')\" style=\"\">"
                                //                                + "</td>"

                                + "<img title='Select Function' src=\"images/Fx icon-01.svg\" class=\"visionETLColMapImage \" onclick=\"selectColumnFun(this,'fromWhereClauseColumn')\" style=\"margin-left: 2px;\">"
                                + "</td>"
                                + "<td width='20%' class=\"sourceJoinColsTd\">"
                                + "<select id=\"OPERATOR_TYPE\"  class=\"sourceColsJoinSelectBox\" onchange=\"disableDdw(this,id)\">"
                                + "<option  value='=' " + ("=".equalsIgnoreCase(operator) ? "selected" : "") + ">=</option>"
                                + "<option  value='!=' " + ("!=".equalsIgnoreCase(operator) ? "selected" : "") + ">!=</option>"
                                + "<option  value='LIKE' " + ("LIKE".equalsIgnoreCase(operator) ? "selected" : "") + ">LIKE</option>"
                                + "<option  value='NOT LIKE' " + ("NOT LIKE".equalsIgnoreCase(operator) ? "selected" : "") + ">NOT LIKE</option>"
                                + "<option  value='IN' " + ("IN".equalsIgnoreCase(operator) ? "selected" : "") + ">IN</option>"
                                + "<option  value='NOT IN' " + ("NOT IN".equalsIgnoreCase(operator) ? "selected" : "") + ">NOT IN</option>"
                                + "<option  value='<' " + ("<".equalsIgnoreCase(operator) ? "selected" : "") + "><</option>"
                                + "<option  value='>' " + (">".equalsIgnoreCase(operator) ? "selected" : "") + ">></option>"
                                + "<option  value='<=' " + ("<=".equalsIgnoreCase(operator) ? "selected" : "") + "><=</option>"
                                + "<option  value='>=' " + (">=".equalsIgnoreCase(operator) ? "selected" : "") + ">>=</option>"
                                + "<option  value='IS' " + ("IS".equalsIgnoreCase(operator) ? "selected" : "") + ">IS</option>"
                                + "<option  value='IS NOT' " + ("IS NOT".equalsIgnoreCase(operator) ? "selected" : "") + ">IS NOT</option>"
                                + "</select>"
                                + "</td>"
                                + "<td width='30%' class=\"sourceJoinColsTd\">"
                                + "<input class='visionColMappingTextarea' type=\"text\" value=\"" + String.valueOf(dataClauseObj.get("staticValue")).replaceAll("'", "&#39;") + "\">"
                                + "</td>"
                                + "<td width='10%' class=\"sourceJoinColsTd\">"
                                + "<select id='andOrOpt'>"
                                + "<option value='AND' " + ("AND".equalsIgnoreCase(operator) ? "selected" : "") + ">AND</option>"
                                + "<option value='OR' " + ("OR".equalsIgnoreCase(operator) ? "selected" : "") + ">OR</option>"
                                + "</select>"
                                + "</td>"
                                + "</tr>";
                    }
                    row++;
                }
            }
            if (!(whereClauseTrString != null && !"".equalsIgnoreCase(whereClauseTrString))) {
                whereClauseTrString = trString;
            }
            whereClauseColsCondition = whereClauseColsCondition + whereClauseTrString + "</tbody></table></div>";
            resultObj.put("whereClauseColsCondition", whereClauseColsCondition);
            resultObj.put("trString", singletrString); // ravi start
        } catch (Exception e) {
        }
        return resultObj.toJSONString();
    }

        @RequestMapping(value = "/normalizeTrfmRules", method = {RequestMethod.GET, RequestMethod.POST})
  public @ResponseBody
    JSONObject normalizeTrfmRules(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = iVisionTransformComponentsService.normalizeTrfmRules(request);
//            joinTablesData = dataPipingService.fetchJoinTablesData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    


           @RequestMapping(value = "/deNormalizeTrfmRules", method = {RequestMethod.GET, RequestMethod.POST})
  public @ResponseBody
    JSONObject deNormalizeTrfmRules(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = iVisionTransformComponentsService.deNormalizeTrfmRules(request);
//            joinTablesData = dataPipingService.fetchJoinTablesData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
           @RequestMapping(value = "/etlProgressBarInfo", method = {RequestMethod.GET, RequestMethod.POST})
  public @ResponseBody
    JSONObject etlProgressBarInfo(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = iVisionTransformComponentsService.etlProgressBarInfo(request);
//            joinTablesData = dataPipingService.fetchJoinTablesData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
            @RequestMapping(value = "/fetchAvailableConnections", method = {RequestMethod.GET, RequestMethod.POST})
  public @ResponseBody
    JSONObject fetchAvailableConnections(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = iVisionTransformComponentsService.fetchAvailableConnections(request);
//            joinTablesData = dataPipingService.fetchJoinTablesData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
    
           @RequestMapping(value = "/pivotComponentTrfmRules", method = {RequestMethod.GET, RequestMethod.POST})
  public @ResponseBody
    JSONObject pivotComponentTrfmRules(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = iVisionTransformComponentsService.pivotComponentTrfmRules(request);
//            joinTablesData = dataPipingService.fetchJoinTablesData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
    
           @RequestMapping(value = "/unpivotComponentTrfmRules", method = {RequestMethod.GET, RequestMethod.POST})
  public @ResponseBody
    JSONObject unpivotComponentTrfmRules(HttpServletRequest request) {

        JSONObject resultObj = new JSONObject();
        try {
            resultObj = iVisionTransformComponentsService.unpivotComponentTrfmRules(request);
//            joinTablesData = dataPipingService.fetchJoinTablesData(request);
        } catch (Exception e) {
        }
        return resultObj;
    }
           
   @RequestMapping(value = "/getComponentInfo", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
     JSONObject getComponentInfo(HttpServletRequest request) {

         JSONObject resultObj = new JSONObject();
         try {
             resultObj = iVisionTransformComponentsService.getComponentInfo(request);
//                     joinTablesData = dataPipingService.fetchJoinTablesData(request);
         } catch (Exception e) {
         }
         return resultObj;
     }
   
   @RequestMapping(value = "/getETLJobPreviewCount", method = {RequestMethod.POST, RequestMethod.GET})
   public @ResponseBody
   JSONObject getETLJobPreviewCount(HttpServletRequest request) {
	   JSONObject resultObj = new JSONObject();
       try {
    	   resultObj = iVisionTransformComponentsService.getETLJobPreviewCount(request);
       } catch (Exception e) {
       }
       return resultObj;
   }
   
   @RequestMapping(value = "/getETLJObReconciliation", method = {RequestMethod.POST, RequestMethod.GET})
   public @ResponseBody
   JSONObject getETLJObReconciliation(HttpServletRequest request) {
	   JSONObject resultObj = new JSONObject();
       try {
           List dataList = iVisionTransformComponentsService.getETLJObReconciliation(request);
           resultObj.put("dataList", dataList);
       } catch (Exception e) {
       }
       return resultObj;
   }
   
   @RequestMapping(value = "/getColumnReconciliation", method = {RequestMethod.POST, RequestMethod.GET})
   public @ResponseBody
   JSONObject getColumnReconciliation(HttpServletRequest request) {
	   JSONObject resultObj = new JSONObject();
       try {
    	   String jobId = request.getParameter("jobId");
    	   String subJobId = request.getParameter("subJobId");
    	   
    	   String sourceTable = request.getParameter("sourceTable");
    	   String targetTable = request.getParameter("targetTable");
    	   String targetTrfmRulesStr = request.getParameter("targetTrfmRules");
    	   JSONObject targetTrfmRules = (JSONObject)JSONValue.parse(targetTrfmRulesStr);
    	   String sourceConnObjStr = request.getParameter("sourceConnObj");
    	   String targetConnObjStr = request.getParameter("targetConnObj");
    	   JSONObject sourceConnObj = (JSONObject)JSONValue.parse(sourceConnObjStr);
    	   JSONObject targetConnObj = (JSONObject)JSONValue.parse(targetConnObjStr);
    	   
    	   	String sessionUserName = (String)request.getSession(false).getAttribute("ssUsername");
       		String orgnId = (String)request.getSession(false).getAttribute("ssOrgId");
       		
       		
           List dataList = iVisionTransformComponentsService.getColumnReconciliation(request, jobId, sourceTable,
        		   targetTable, targetTrfmRules, sourceConnObj, targetConnObj, sessionUserName, orgnId, subJobId);
           resultObj.put("dataList", dataList);
       } catch (Exception e) {
       }
       return resultObj;
   }   
   
   
   @RequestMapping(value = "/etlGridExport", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   void etlGridExport(HttpServletRequest request, HttpServletResponse response) {

      
       try {
            iVisionTransformComponentsService.etlGridExport(request, response); // executeAlterTable
       } catch (Exception e) {
           e.printStackTrace();
       }
     
   }
   
   @RequestMapping(value = "/createTemPreviewTables", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   void createTemPreviewTables(HttpServletRequest request) {

      
       try {
            iVisionTransformComponentsService.createTemPreviewTables(request); // executeAlterTable
       } catch (Exception e) {
           e.printStackTrace();
       }
     
   }
   
   @RequestMapping(value = "/deleteTemPreviewTables", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   void deleteTemPreviewTables(HttpServletRequest request) {
       try {
            iVisionTransformComponentsService.deleteTemPreviewTables(request); // executeAlterTable
       } catch (Exception e) {
           e.printStackTrace();
       }
     
   }
   
   
   @RequestMapping(value = "/checkDataTypeValidations", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject checkDataTypeValidations(HttpServletRequest request) {
	   JSONObject resultObj =  new JSONObject();
       try {
    	   resultObj = iVisionTransformComponentsService.checkDataTypeValidations(request); // executeAlterTable
       } catch (Exception e) {
           e.printStackTrace();
       }
     return resultObj;
   }
   
   

   

   @RequestMapping(value = "/processScheduledJobFlag", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject processScheduledJobFlag(HttpServletRequest request) {
       JSONObject scheduledJobObj = new JSONObject();
       try {

           scheduledJobObj = jobSchedulingProcessService.processScheduledJobFlag(request);

       } catch (Exception e) {
       }
       return scheduledJobObj;
   }
      
   
   @RequestMapping(value = "/fetchJobScheduleInfo", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   Map fetchJobScheduleInfo(HttpServletRequest request) {
       Map scheduledJobObj = new LinkedHashMap<>();
       try {
    	   
           scheduledJobObj = jobSchedulingProcessService.fetchJobScheduleInfo(request);

       } catch (Exception e) {
       }
       return scheduledJobObj;
   }
   
   
   @RequestMapping(value = "/validateMergeComponentTrfmRules", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody JSONObject validateMergeComponentTrfmRules(HttpServletRequest request) {
      JSONObject resultObj =  new JSONObject();
       try {
    	   resultObj = jobSchedulingProcessService.validateMergeComponentTrfmRules(request);

       } catch (Exception e) {
       }
       return resultObj;
   }
   
   
   @RequestMapping(value = "/apiCompnentTrfmRules", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody JSONObject apiCompnentTrfmRules(HttpServletRequest request) {
      JSONObject resultObj =  new JSONObject();
       try {
    	   resultObj = iVisionTransformComponentsService.apiCompnentTrfmRules(request);

       } catch (Exception e) {
       }
       return resultObj;
   }
   
   
   @RequestMapping(value = "/performDataProfilingFromETL", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody JSONObject performDataProfilingFromETL(HttpServletRequest request) {
      JSONObject resultObj =  new JSONObject();
       try {
    	   resultObj = iVisionTransformComponentsService.performDataProfilingFromETL(request);

       } catch (Exception e) {
       }
       return resultObj;
   }
   @RequestMapping(value = "/performDataCleansingFromETL", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody JSONObject performDataCleansingFromETL(HttpServletRequest request) {
      JSONObject resultObj =  new JSONObject();
       try {
    	   resultObj = iVisionTransformComponentsService.performDataCleansingFromETL(request);

       } catch (Exception e) {
       }
       return resultObj;
   }
   @RequestMapping(value = "/performVendorValidation", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody JSONObject performVendorValidation(HttpServletRequest request) {
      JSONObject resultObj =  new JSONObject();
       try {
    	   resultObj = iVisionTransformComponentsService.performVendorValidation(request);

       } catch (Exception e) {
       }
       return resultObj;
   }
   
   @RequestMapping(value = "/etlReferenceDataExtraction", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody JSONObject etlReferenceDataExtraction(HttpServletRequest request) {
      JSONObject resultObj =  new JSONObject();
       try {
    	   resultObj = iVisionTransformComponentsService.etlReferenceDataExtraction(request);

       } catch (Exception e) {
       }
       return resultObj;
   }
   
   @RequestMapping(value = "/getTableColumnsList", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject getTableColumnsList(HttpServletRequest request) { 
       JSONObject resultObj = new JSONObject();
       try {
            resultObj = iVisionTransformComponentsService.getTableColumnsList(request);
       } catch (Exception e) {
       }
       return resultObj;
   }
   
   
   @RequestMapping(value = "/updateMakedColumns", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject updateMakedColumns(HttpServletRequest request) { 
       JSONObject resultObj = new JSONObject();
       try {
     			  resultObj = iVisionTransformComponentsService.updateMakedColumns(request);
       } catch (Exception e) {
       }
       return resultObj;
   }
   
   @RequestMapping(value = "/fetchMaskedColumns", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject fetchMaskedColumns(HttpServletRequest request) { 
       JSONObject resultObj = new JSONObject();
       try {
     			  //resultObj = iVisionTransformComponentsService.fetchMaskedColumns(request);
       } catch (Exception e) {
       }
       return resultObj;
   }
   
   
   @RequestMapping(value = "/scheduleProcessJob", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject scheduleProcessJob(HttpServletRequest request) {
       JSONObject resultObj = new JSONObject();
       String result = "";
       try {

           resultObj = jobSchedulingProcessService.scheduleProcessJob(request);

       } catch (Exception e) {
       }
       return resultObj;
   }
   
   @RequestMapping(value = "/scheduleStoredProcedure", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject scheduleStoredProcedure(HttpServletRequest request) {
       JSONObject resultObj = new JSONObject();
       String result = "";
       try {
           resultObj = procSchedulingService.scheduleStoredProcedure(request);
        } catch (Exception e) {
        }
       return resultObj;
   }
   
   @RequestMapping(value = "/loadScheduledJobs", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject loadScheduledJobs(HttpServletRequest request) {
       JSONObject scheduledJobObj = new JSONObject();
       String result = "";
       try {

           scheduledJobObj = jobSchedulingProcessService.getActiveScheduledJobs(request);

       } catch (Exception e) {
       }
       return scheduledJobObj;
   }
   
   @RequestMapping(value = "/loadScheduledProcs", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject loadScheduledProcs(HttpServletRequest request) {
       JSONObject scheduledProcsObj = new JSONObject();
       String result = "";
       try {

           scheduledProcsObj = procSchedulingService.getScheduledProcs(request);

       } catch (Exception e) {
       }
       return scheduledProcsObj;
   }
   
   @RequestMapping(value = "/removeProcFromScheduler", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject removeProcFromScheduler(HttpServletRequest request) {
       JSONObject scheduledProcsObj = new JSONObject();
       try {

           scheduledProcsObj = procSchedulingService.processScheduledProc(request);

       } catch (Exception e) {
       }
       return scheduledProcsObj;
   }
   
   @RequestMapping(value = "/fetchRemoveScheduledProc", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   JSONObject fetchRemoveProcFromScheduler(HttpServletRequest request) {
       JSONObject scheduledProcsObj = new JSONObject();
       try {

           scheduledProcsObj = procSchedulingService.fetchRemoveProcFromScheduler(request);

       } catch (Exception e) {
       }
       return scheduledProcsObj;
   }
   
   @RequestMapping(value = "/fetchProcScheduleInfo", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   Map fetchProcScheduleInfo(HttpServletRequest request) {
       Map scheduledProcObj = new LinkedHashMap<>();
       try {
    	   
           scheduledProcObj = procSchedulingService.fetchProcScheduleInfo(request);

       } catch (Exception e) {
       }
       return scheduledProcObj;
   }
   
   @RequestMapping(value = "/fetchProcScheduleLog", method = {RequestMethod.GET, RequestMethod.POST})
   public @ResponseBody
   Map fetchProcScheduleLog(HttpServletRequest request) {
       Map scheduledProcObj = new LinkedHashMap<>();
       try {
    	   
           scheduledProcObj = procSchedulingService.fetchProcScheduleLog(request);

       } catch (Exception e) {
       }
       return scheduledProcObj;
   }
   
   @RequestMapping(value = "/insertDivIntoDB", method = {RequestMethod.POST, RequestMethod.GET}, produces = "text/plain;charset=UTF-8")
    public @ResponseBody
    String insertDivIntoDB(HttpServletRequest request) {
        String result = "";
        try {
            result = iVisionTransformComponentsService.insertDivIntoDB(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
   
   @RequestMapping(value = "/validateComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
   public JSONObject validateComponentTrfmRules(HttpServletRequest request) {
       JSONObject resultObject = new JSONObject();
       try {
           resultObject = iVisionTransformComponentsService.validateComponentTrfmRules(request);
       } catch (Exception e) {
           e.printStackTrace();
       }
       return resultObject;
   }

   @RequestMapping(value = "/fetchComponentTrfmRules", method = {RequestMethod.POST, RequestMethod.GET})
   public JSONObject fetchComponentTrfmRules(HttpServletRequest request) {
       JSONObject resultObject = new JSONObject();
       try {
           resultObject = iVisionTransformComponentsService.fetchComponentTrfmRules(request);
       } catch (Exception e) {
           e.printStackTrace();
       }
       return resultObject;
   }
   
	@RequestMapping(value = "/sapHanaLoadTrfmRules", method = { RequestMethod.POST, RequestMethod.GET })
	public JSONObject sapHanaLoadTrfmRules(HttpServletRequest request) {
		JSONObject resultObject = new JSONObject();
		try {
			resultObject = iVisionTransformComponentsService.sapHanaLoadTrfmRules(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;

	}
}
