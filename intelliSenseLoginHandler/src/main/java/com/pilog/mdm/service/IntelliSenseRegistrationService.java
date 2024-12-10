/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.pilog.mdm.utilities.PilogUtilities;
import com.pilog.mdm.DAO.IntelliSenseLoginHandlerDAO;
import com.pilog.mdm.DAO.IntelliSenseRegistrationDAO;
import com.pilog.mdm.DTO.RegistrationDTO;
import com.pilog.mdm.pojo.BApplProperties;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author Kiran Raj
 */
@Service
public class IntelliSenseRegistrationService {

    @Autowired
    private IntelliSenseLoginHandlerDAO loginHandlerDAO;
    private PilogUtilities VisionUtills = new PilogUtilities();
    @Autowired
    private IntelliSenseRegistrationDAO registrationDAO;

    /*public JSONObject registerUser(RegistrationDTO registrationDTO, HttpServletRequest request) {
        JSONObject jsResponse = new JSONObject();
        try {
            jsResponse = registrationDAO.registerUser(registrationDTO, request);
            if (jsResponse.containsKey("status")) {
                if (jsResponse != null && (int) jsResponse.get("status") == 2) {
                    String resultString = "USERS_EXCEEDED";
                    BApplProperties errorMessageProps = (BApplProperties) loginHandlerDAO.getApplProperties(resultString);
                    jsResponse.put("MessageFlag", false);
                    jsResponse.put("Message", errorMessageProps.getId().getProcessValue());
                }
            }
        } catch (Exception e) {
            jsResponse.put("Message", "Unable to register user.");
            jsResponse.put("MessageFlag", false);
            e.printStackTrace();
        }
        return jsResponse;
    }*/

    //onkar365
    public JSONObject registerUser(RegistrationDTO registrationDTO, HttpServletRequest request) {
        JSONObject jsResponse = new JSONObject(); 
        try {
            jsResponse = registrationDAO.registerUser(registrationDTO, request);
            if (jsResponse.containsKey("status")) {
                if (jsResponse != null && (int) jsResponse.get("status") == 2) {
                    String resultString = "USERS_EXCEEDED";
                    BApplProperties errorMessageProps = (BApplProperties) loginHandlerDAO.getApplProperties(resultString);
                    jsResponse.put("MessageFlag", false);
                    jsResponse.put("Message", errorMessageProps.getId().getProcessValue());
                }
            }
        } catch (Exception e) {
            jsResponse.put("Message", "Unable to register user.");
            jsResponse.put("MessageFlag", false);
            e.printStackTrace();
        }
        return jsResponse;
    }
    //onkar365

    public JSONObject passwordauthorisation(HttpServletRequest request) {
        return registrationDAO.passwordauthorisation(request);
    }

    public String getBreadCrumbs(HttpServletRequest request, String componentId) {
        return registrationDAO.getBreadCrumbs(request, componentId);
    }

    public JSONObject getUserData(HttpServletRequest request) {
        return registrationDAO.getUserData(request);
    }

    public String updateProfileData(HttpServletRequest request, JSONObject item) {
        return registrationDAO.updateProfileData(request, item);
    }

    public JSONObject changepassword(HttpServletRequest request) {
        return registrationDAO.changepassword(request);
    }

    public String forgotpassword(HttpServletRequest request) {
        return registrationDAO.forgotpassword(request);
    }

    public JSONObject getPersDetails(HttpServletRequest request, String username) {
        return registrationDAO.getPersDetails(request, username);
    }

//onkar 365
    public JSONObject sendActivationMail(HttpServletRequest request, RegistrationDTO registrationDTO) {
        return registrationDAO.sendActivationMail(request, registrationDTO);
    }

    public JSONObject updateUserStatus(HttpServletRequest request) {

        return registrationDAO.updateUserStatus(request);

    }

//onkar 365   
    public JSONObject emailOtpVerificationService(HttpServletRequest request) throws Exception {
        return registrationDAO.emailOTP(request);
    }

    //16222
    public JSONObject checkOtpValService(HttpServletRequest request) {
        return registrationDAO.checkOtp(request);
    }
    public String getRoleSelectionTypes(HttpServletRequest request){
        String resultStr="";
        try {
            BApplProperties resultObj = (BApplProperties) loginHandlerDAO.getApplProperties("DXP_ROLE_TYPES");
           if (resultObj != null) {
               String selectionTypes = resultObj.getId().getProcessValue();
               if(selectionTypes != null && !"".equalsIgnoreCase(selectionTypes) && !"null".equalsIgnoreCase(selectionTypes)){
                   if(selectionTypes.contains(",")){
                       String[] typesArr = selectionTypes.split(",");
                       resultStr = "<select name=\"role\" class=\"form-control\" id=\"role\" title=\"Please select role\">";
                       for(int i=0;i<typesArr.length;i++){
                           resultStr += "<option value='"+typesArr[i]+"'>"+typesArr[i]+"</option>";
                       }
                       resultStr += "</select>";
                               
                   }
               }
               
           }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultStr;
    }
    

    public JSONObject getProcessLoginAuth(HttpServletRequest request) {
        return registrationDAO.getProcessLoginAuth(request);
    }
     
       public JSONObject getProcessLoginOtpAuth(HttpServletRequest request) {
        return registrationDAO.getProcessLoginOtpAuth(request);
    }
       
       
       public JSONObject checkForRole(HttpServletRequest request) {
			return registrationDAO.checkForRole(request) ;
		 }
	 public JSONObject createUser(HttpServletRequest request, String griddata) {
	        JSONObject jsResponse = new JSONObject();
	        try {
	            //JSONObject griddata= request.getParameter("gridData");
	            jsResponse = registrationDAO.createUser(request, (JSONObject) new JSONParser().parse(griddata));
	            if (jsResponse == null) {
	                jsResponse.put("status", 0);
	                jsResponse.put("message", "Unable to create user");

	            } else if ((int) jsResponse.get("status") == 1) {
	                RegistrationDTO registrationDTO = (RegistrationDTO) jsResponse.get("dto");
	                //dashBoardsDAO.logMail(registrationDTO, request);
	                jsResponse.remove("dto");
	            } else if ((int) jsResponse.get("status") == 0) {

	            } else if ((int) jsResponse.get("status") == 2) {

	                String resultString = "USERS_EXCEEDED";
	                BApplProperties errorMessageProps = (BApplProperties) loginHandlerDAO.getApplProperties(resultString);
	                //errmessage = errorMessageProps.getId().getProcessValue();
	                jsResponse.put("status", 2);

	                jsResponse.put("message", errorMessageProps.getId().getProcessValue());

	            }

	        } catch (org.json.simple.parser.ParseException ex) {
	            jsResponse = new JSONObject();
	            jsResponse.put("status", 0);
	            jsResponse.put("message", "Unable to create user");
	        }
	        return jsResponse;
	    }
	 public Integer userExists(HttpServletRequest request) {
	        return registrationDAO.userExists(request);
	    }
	

    
    
}
