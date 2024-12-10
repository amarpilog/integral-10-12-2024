/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.controller;

import com.pilog.mdm.utilities.PilogEncryption;

import com.pilog.mdm.DTO.RegistrationDTO;
import com.pilog.mdm.service.IntelliSenseMenuService;
import com.pilog.mdm.service.IntelliSenseLoginHandlerService;
import com.pilog.mdm.service.IntelliSenseRegistrationService;
import com.pilog.mdm.utilities.PilogUtilities;
import java.util.Enumeration;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import static jxl.biff.BaseCellFeatures.logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 *
 * @author Kiran Raj
 */
@Controller
public class IntelliSenseRegistrationController {

    @Autowired
    private IntelliSenseRegistrationService registrationService;
    @Autowired
    private IntelliSenseLoginHandlerService loginHandlerService;
    private PilogUtilities VisionUtills = new PilogUtilities();

    @Autowired
    private IntelliSenseMenuService menuService;

    @RequestMapping(value = {"/cloudRegistrationForm"}, method = {RequestMethod.POST, RequestMethod.GET})
    public String cloudRegistrationForm(ModelMap model, HttpServletRequest request) {
        try {
            model.addAttribute("secretKey", PilogEncryption.secretKey);
             model.put("roleTypes", registrationService.getRoleSelectionTypes(request));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "registration";
    }
    //onkar 365
    @RequestMapping(value = "/register", method = RequestMethod.POST)
    public @ResponseBody
    JSONObject register(HttpServletRequest request, ModelMap model) {
        JSONObject resultObj = new JSONObject();
        try {
            String uri1 = request.getParameter("uri");
            JSONObject basicData = new JSONObject();
            String jsonString = request.getParameter("basicData");
            if (jsonString != null && !"".equalsIgnoreCase(jsonString) && !"null".equalsIgnoreCase(jsonString)) {
                basicData = (JSONObject) JSONValue.parse(jsonString);
            }
            RegistrationDTO registrationDTO = setAllToRegistrationDTO(request, basicData);
            resultObj = registrationService.registerUser(registrationDTO, request);
//            if (resultObj != null && !resultObj.isEmpty() && (Boolean) resultObj.get("MessageFlag")) {
//                registrationService.sendActivationMail(request, registrationDTO);//onkar
//            }

        } catch (Exception e) {
            resultObj.put("Message", e.getLocalizedMessage());
            resultObj.put("MessageFlag", false);
            logger.error(e.getLocalizedMessage());
        }

        return resultObj;
    }
    //onkar 365

    public RegistrationDTO setAllToRegistrationDTO(HttpServletRequest request, JSONObject basicData) {
        RegistrationDTO registrationDTO = new RegistrationDTO();
        String userFirstName = "";
        try {
//            String dateofbirth = request.getParameter("year") + "-" + request.getParameter("month") + "-" + request.getParameter("date");
            String role = request.getParameter("role");
            String userNameReq = (String) basicData.get("rsUsername");
            if(userNameReq !=  null && !"".equalsIgnoreCase(userNameReq) && !"null".equalsIgnoreCase(userNameReq)
                    && userNameReq.contains("_")){
                String[] detalisStringArr = userNameReq.split("_");
                userFirstName = detalisStringArr[0];
            }
            String password = "";
            if (role != null && !role.equalsIgnoreCase("") && role.equalsIgnoreCase("FUNCT_CONSULTANT")) {
                password = "QweAsdZxc";
            } else {
                password = (String) basicData.get("password") != null ? (String) basicData.get("password") : "";
            }
            registrationDTO.setAdditional_role((String) basicData.get("add_role") != null ? (String) basicData.get("add_role") : "");
            registrationDTO.setAddress1((String) basicData.get("City") != null ? (String) basicData.get("City") : "");
            registrationDTO.setAddress2((String) basicData.get("State") != null ? (String) basicData.get("State") : "");
            registrationDTO.setConfirm_password((String) basicData.get("confirm_password") != null ? (String) basicData.get("confirm_password") : "");
            registrationDTO.setCountry((String) basicData.get("country") != null ? (String) basicData.get("country") : "");
            registrationDTO.setDate((String) basicData.get("dob") != null ? (String) basicData.get("dob") : "");
            registrationDTO.setEmail_id((String) basicData.get("email_id") != null ? (String) basicData.get("email_id") : "");
            registrationDTO.setExperience_summary((String) basicData.get("Company") != null ? (String) basicData.get("Company") : "");
            registrationDTO.setFirst_name(userFirstName);

            registrationDTO.setLast_name(userFirstName);
            registrationDTO.setLocale(request.getParameter("locale"));
            registrationDTO.setMobile_number((String) basicData.get("mobile_number") != null ? (String) basicData.get("mobile_number") : "");
            registrationDTO.setMiddle_name((String) basicData.get("age") != null ? (String) basicData.get("age") : "");
            registrationDTO.setMonth((String) basicData.get("month") != null ? (String) basicData.get("month") : "");
            registrationDTO.setNick_name((String) basicData.get("jobtitle") != null ? (String) basicData.get("jobtitle") : "");
            registrationDTO.setPassword(password);

            registrationDTO.setPhone_number((String) basicData.get("ZipCode") != null ? (String) basicData.get("ZipCode") : "");
            registrationDTO.setPlant((String) basicData.get("plant") != null ? (String) basicData.get("plant") : "1000");
            registrationDTO.setInstance((String) basicData.get("instance") != null ? (String) basicData.get("instance") : "100");
            registrationDTO.setReport_to((String) basicData.get("report_to") != null ? (String) basicData.get("report_to") : "MM_MANAGER");
            registrationDTO.setRole(role);
            //registrationDTO.setUser_name((String) basicData.get("user_name") != null ? (String) basicData.get("user_name") : "");
            registrationDTO.setUser_name((String) basicData.get("rsUsername") != null ? (String) basicData.get("rsUsername") : "");
            registrationDTO.setYear((String) basicData.get("year") != null ? (String) basicData.get("year") : "");
            registrationDTO.setFilepath(request.getParameter("filepath"));
            registrationDTO.setOrgName((String) basicData.get("orgnName") != null ? (String) basicData.get("orgnName") : "Vision");
            registrationDTO.setDate_of_birth((String) basicData.get("dob") != null ? (String) basicData.get("dob") : "");
            registrationDTO.setPurposeofReg((String) basicData.get("gender") != null ? (String) basicData.get("gender") : "");
            //usr_orgid
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }

        return registrationDTO;
    }
    @RequestMapping(value = "/userResetPassword", method = {RequestMethod.GET, RequestMethod.POST})
    public String changepass(HttpServletRequest request, ModelMap model) {

        JSONObject jSONObject = null;
        JSONObject labelobj = new JSONObject();
        jSONObject = registrationService.passwordauthorisation(request);
        String headerdata = "Change Password";
        model.addAttribute("value", 3);
        model.addAttribute("passwordval", jSONObject.get("passwordvalidation"));
        HttpSession session = request.getSession();
        session.setAttribute("ssOrgId", request.getParameter("ssOrgId"));
        session.setAttribute("ssLocale", request.getParameter("en_US"));
        labelobj = new PilogUtilities().getMultilingualObject(session);
        model.addAttribute("labelobj", labelobj);
        model.addAttribute("headerdata", headerdata);
        model.addAttribute("secretKey", PilogEncryption.secretKey);
//        model.addAttribute("breadCrumb", registrationService.getBreadCrumbs(request, "SETTINGS_CHANGE_PASSWORD"));

        return "ChangePassword";
    }

    @RequestMapping(value = "/userProfile", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject getUserData(HttpServletRequest request, ModelMap model) {
        JSONObject resultObj = new JSONObject();
        try {
            JSONObject userProfileDataObj = registrationService.getUserData(request);
            resultObj.put("userData", userProfileDataObj);
            HttpSession session = request.getSession();
            session.setAttribute("ssOrgId", request.getParameter("ssOrgId"));
            session.setAttribute("ssLocale", request.getParameter("en_US"));
            JSONObject labelobj = new PilogUtilities().getMultilingualObject(session);
            resultObj.put("labelobj", labelobj);
            resultObj.put("DOB", userProfileDataObj.get("DOB").toString());
//            resultObj.put("breadCrumb", registrationService.getBreadCrumbs(request, "SETTINGS_PROFILE"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = "/updateProfileData", method = RequestMethod.POST)
    public @ResponseBody
    JSONObject updateProfileData(HttpServletRequest request, @RequestParam("jsonData") String jsonDataString) {
        JSONObject resultObj = new JSONObject();
        try {

            JSONObject basicData = (JSONObject) JSONValue.parse(jsonDataString);
            if (basicData != null && !basicData.isEmpty()) {
                String result = registrationService.updateProfileData(request, basicData);
                resultObj.put("result", result);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @RequestMapping(value = "/changepassword", method = RequestMethod.POST, produces = "application/json")
    public @ResponseBody
    JSONObject changepassword(HttpServletRequest request) {
        JSONObject changePwdObj = new JSONObject();

        try {
            changePwdObj = registrationService.changepassword(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return changePwdObj;
    }

    @RequestMapping(value = "/forgotPass", method = {RequestMethod.GET, RequestMethod.POST})
    public JSONObject forgotPass(HttpServletRequest request) {
        JSONObject forgotPwdObj = new JSONObject();
        try {
            forgotPwdObj.put("value", 1);
            forgotPwdObj.put("secretKey", PilogEncryption.secretKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return forgotPwdObj;
    }

    @RequestMapping(value = "/forgotPassword", method = RequestMethod.POST)
    public @ResponseBody
    JSONObject forgotPassword(HttpServletRequest request) {
        String validateString = "true";
        JSONObject forgotPwdObj = new JSONObject();
        try {
            validateString = registrationService.forgotpassword(request);
            forgotPwdObj.put("response", validateString);
        } catch (Exception e) {
            logger.error("Exception In emailIdValidate:", e);
        }
        return forgotPwdObj;
    }
    //userActivation through mail ms365.
    @RequestMapping(value = {"/activateUser"}, method = {RequestMethod.GET, RequestMethod.POST})
    String activateUser(HttpServletRequest request, ModelMap model) {
        JSONObject resultObj = new JSONObject();
        String redirectUrl = "";
        try {

            String userActivationStatus = request.getParameter("activationStatus");//ms365
            HttpSession session = request.getSession(false);
            if (session != null) {
                System.out.println("homePage:::" + session.getId());
                System.out.println("ssUsername::" + session.getAttribute("ssUsername"));
            }
            String sideMenuStr = menuService.getSideMenu(request);
            request.getSession(false).setAttribute("sideMenuStr", sideMenuStr);
            model.put("sideMenuStr", sideMenuStr);
            model.put("secretKey", PilogEncryption.secretKey);

            resultObj = registrationService.updateUserStatus(request);
            if (resultObj != null && !resultObj.isEmpty()) {
                boolean status = (boolean) resultObj.get("status");
                String message = (String) resultObj.get("message");
                if (status) {
                    redirectUrl = "Home";
                    model.addAttribute("activationStatus", "Y");//ms365
                    model.addAttribute("activationDispMessage", message);//ms365
                } else {
                    redirectUrl = "pageError";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //return "redirect:" + redirectUrl;
        return redirectUrl;
    }
    @RequestMapping(value = "/emailOtpVerification", method = RequestMethod.POST)
    public @ResponseBody
    String emailOtpVerification(HttpServletRequest request) {
        JSONObject otpObj = new JSONObject();
        try {
             otpObj = registrationService.emailOtpVerificationService(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return otpObj.toJSONString();
    }

    @RequestMapping(value = "/checkOtpVal", method = RequestMethod.POST)
    public @ResponseBody
    String checkOtpVal(HttpServletRequest request) {
        JSONObject otpObj = registrationService.checkOtpValService(request);
        return otpObj.toJSONString();
    }
    //otp verification//
    
    @RequestMapping(value = "/accountActivation", method = RequestMethod.POST)
    public @ResponseBody
    String accountActivation(HttpServletRequest request
    ) {
        JSONObject accountActivationObj = new JSONObject();
        try {
            RegistrationDTO registrationDTO = new RegistrationDTO();
            registrationDTO.setEmail_id(request.getParameter("email"));
//            accountActivationObj = registrationService.accountActivationService(request, registrationDTO);
        } catch (Exception e) {
        }
        return accountActivationObj.toJSONString();
    }
    @RequestMapping(value = {"/activateUserThroughLink"}, method = {RequestMethod.GET, RequestMethod.POST})//8322
    public @ResponseBody //25322 added responsebody, String to JSONOBJ
    JSONObject activateUserThroughLink(HttpServletRequest request, ModelMap model
    ) {
        JSONObject response = new JSONObject();
        try {
            response = registrationService.sendActivationMail(request, new RegistrationDTO());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
}
    @RequestMapping(value = "/getProcessLoginAuth", method = RequestMethod.POST)
    public @ResponseBody
    JSONObject getProcessLoginAuth(HttpServletRequest request) {
        JSONObject response = new JSONObject();
        try {
            response = registrationService.getProcessLoginAuth(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }
    
     @RequestMapping(value = "/getProcessLoginOtpAuth", method = RequestMethod.POST)
    public @ResponseBody
    JSONObject getProcessLoginOtpAuth(HttpServletRequest request) {
        JSONObject response = new JSONObject();
        try {
            response = registrationService.getProcessLoginOtpAuth(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }
     
     
     
     
     @RequestMapping(value = "/checkForRole", method = {RequestMethod.POST, RequestMethod.GET})
	    public @ResponseBody
	    JSONObject checkForRole(HttpServletRequest request){
	        JSONObject resultObj = new JSONObject();
	        try { 
	            resultObj=registrationService.checkForRole(request);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return resultObj;
	    }
	 
	  
	 @RequestMapping(value = "/createUser", method = RequestMethod.POST)
	    public @ResponseBody
	    JSONObject createUser(HttpServletRequest request,
	            @RequestParam String griddata
	    ) {
	        return registrationService.createUser(request, griddata);
	    }
	 
	 @RequestMapping(value = "/checkUserAvailability", method = RequestMethod.POST)
	    public @ResponseBody
	    JSONObject userExists(HttpServletRequest request) {
	        JSONObject result = new JSONObject();
	        try { 
	         result.put("status",registrationService.userExists(request));
	        }
	        catch(Exception e) {
	        	e.printStackTrace();
	        }
	    return result;
	    }
	
    
    

}//class
