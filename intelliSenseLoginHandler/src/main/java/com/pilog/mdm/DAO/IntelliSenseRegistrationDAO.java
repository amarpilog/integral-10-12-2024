/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;

import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogEncryption;
import com.pilog.mdm.utilities.PilogUtilities;
import com.pilog.mdm.numbergeneration.NumberGeneration;
import com.pilog.mdm.DTO.RegistrationDTO;
import com.pilog.mdm.access.DataAccess;
//import com.pilog.mdm.cloud.access.DataAccess;
//import com.pilog.mdm.cloud.access.DataAccess;
import com.pilog.mdm.pojo.BApplProperties;
import com.pilog.mdm.pojo.BCountry;
import com.pilog.mdm.pojo.BLanguage;
import com.pilog.mdm.pojo.DalMailConfig;
import com.pilog.mdm.pojo.DalNtfnTemplate;
import com.pilog.mdm.pojo.DalRoleHelp;
import com.pilog.mdm.pojo.DalRoleNvgn;
import com.pilog.mdm.pojo.OrgnStructure;
import com.pilog.mdm.pojo.SAuthConfiguration;
import com.pilog.mdm.pojo.SAuthHistory;
import com.pilog.mdm.pojo.SAuthHistoryId;
import com.pilog.mdm.pojo.SAuthorisation;
import com.pilog.mdm.pojo.SAuthorisationId;
import com.pilog.mdm.pojo.SPersDetail;
import com.pilog.mdm.pojo.SPersOrgnLang;
import com.pilog.mdm.pojo.SPersOrgnLangId;
import com.pilog.mdm.pojo.SPersProfile;
import com.pilog.mdm.pojo.SPersProfileId;
import com.pilog.mdm.pojo.SPersStgAuth;
import com.pilog.mdm.pojo.SPersnolisation;
import com.pilog.mdm.pojo.SPersnolisationId;
import com.pilog.mdm.pojo.SPersonnel;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import oracle.sql.RAW;
import org.apache.commons.lang.RandomStringUtils;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author Kiran Raj
 */
@Repository
public class IntelliSenseRegistrationDAO {

    @Autowired
    private DataAccess access;
    @Value("${command.path}")
    private String macCommand;
    @Value("${mail.user.username}")
    private String mailUsername;
    private static final Logger logger = LoggerFactory.getLogger(IntelliSenseRegistrationDAO.class);
    private PilogUtilities VisionUtills = new PilogUtilities();
    @Autowired
    private IntelliSenseSheduleDAO cloudSheduleDAO;
    @Autowired
    private NumberGeneration numGen;
    @Autowired
    private IntelliSenseLoginHandlerDAO loginHandlerDAO;

    //onkar365
    @Transactional(propagation = Propagation.REQUIRED)
    public JSONObject registerUser(RegistrationDTO registrationDTO, HttpServletRequest request) {
        boolean status = checkingNoOfUserCount(registrationDTO, request);
        JSONObject resultObject = new JSONObject();
        try {
            boolean ipResult = false;
            String role = registrationDTO.getRole();
            if (status) {
                if (role != null && !role.equalsIgnoreCase("")) //                        && role.equalsIgnoreCase("MM_CAAS_USER"))
                {
                    ipResult = compareIpOfTrailUser(registrationDTO, request);
                }
                if (!ipResult) {
                    resultObject = (JSONObject) savePersDetail(registrationDTO, request);
                } else {
                    resultObject.put("Message", "Already User created with this Email.");
                    resultObject.put("MessageFlag", false);
                }
            } else {
                resultObject.put("status", 2);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exception in registerUser of RegistrationDAO ", e.getLocalizedMessage());
            resultObject.put("Message", e.getLocalizedMessage());
            resultObject.put("MessageFlag", false);
        }
        return resultObject;
    }
    //onkar365

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean checkingNoOfUserCount(RegistrationDTO registrationDTO, HttpServletRequest request) {
        boolean isProcess = false;
        String orngName = registrationDTO.getOrgName();
        String ssOrgnId = getOrgnIdByName(orngName);
        String role = registrationDTO.getRole();
        String userName = registrationDTO.getUser_name();

        try {
            if (userName != null && !"".equalsIgnoreCase(userName) && !"null".equalsIgnoreCase(userName)
                    && userName.contains("REQ")) {
                role = "MM_REQUESTOR";
            } else if (userName != null && !"".equalsIgnoreCase(userName) && !"null".equalsIgnoreCase(userName)
                    && userName.contains("APP")) {
                role = "MM_APPROVER";
            } else if (userName != null && !"".equalsIgnoreCase(userName) && !"null".equalsIgnoreCase(userName)
                    && userName.contains("MGR")) {
                role = "MM_MANAGER";
            } else if (userName != null && !"".equalsIgnoreCase(userName) && !"null".equalsIgnoreCase(userName)
                    && userName.contains("STD")) {
                role = "MM_STEWARD";
            }else if (userName != null && !"".equalsIgnoreCase(userName) && !"null".equalsIgnoreCase(userName)
                    && userName.contains("ADM")) {
                role = "ADMIN";
            }
            else {
            	role = "MM_MANAGER"; 
            }
            registrationDTO.setRole(role);
            String roleUsersQuery = "SELECT NO_OF_USERS FROM C_ROLE_USERS WHERE ORGN_ID =:ORGN_ID AND ROLE_ID =:ROLE_ID";
            Map<String, Object> roleUsersMap = new HashMap<>();
            roleUsersMap.put("ORGN_ID", ssOrgnId);
            roleUsersMap.put("ROLE_ID", role);
            List roleUsersList = access.sqlqueryWithParams(roleUsersQuery, roleUsersMap);
            BigDecimal next = new BigDecimal(0);
            for (Iterator iterator = roleUsersList.iterator(); iterator.hasNext();) {
                next = (BigDecimal) iterator.next();
                System.out.println("The roleUsersList value is:::" + next);
            }
            int roleUsersCount = next.intValue();

            String noOfUserQuery = "SELECT DISTINCT USER_NAME FROM RPRT_PERS_DETAILS WHERE "
            		//+ "ROLE_ID =:ROLE_ID AND "
            		+ "ORGN_ID =:ORGN_ID AND STATUS ='ACTIVE'";
            Map<String, Object> persDetailMap = new HashMap<>();
            persDetailMap.put("ORGN_ID", ssOrgnId);
            //persDetailMap.put("ROLE_ID", role);
            List noOfUserList = access.sqlqueryWithParams(noOfUserQuery, persDetailMap);
            if (noOfUserList != null && !noOfUserList.isEmpty()) {
                if (noOfUserList.size() < roleUsersCount) {
                    isProcess = true;
                } else {
                    isProcess = false;
                }
            } else {
                isProcess = true;
            }
        } // }
        catch (Exception e) {
            e.printStackTrace();
        }
        return isProcess;
    }

    @Transactional
    public boolean compareIpOfTrailUser(RegistrationDTO registrationDTO, HttpServletRequest request) {
        //23222
        boolean result = false;
        try {
            String emailEntered = registrationDTO.getEmail_id();
            String emailListQuery = "select email from SPersDetail";
            List verifiedEmailList = access.queryWithParams(emailListQuery, Collections.EMPTY_MAP);

            for (Object email : verifiedEmailList) {
                String emailStr = (String) email;
                if (emailStr.equalsIgnoreCase(emailEntered)) {
                    result = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Transactional
    public String getOrgnIdByName(String orgname) {
        String orgnId = "";
        try {
            String query = "select orgnId from OrgnStructure where name=:name";
            Map<String, Object> map = new HashMap<>();
            map.put("name", orgname);
            List list = access.queryWithParams(query, map);
            if (list != null && !list.isEmpty()) {
                orgnId = (String) list.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return orgnId;
    }

    @Transactional
    public SPersDetail getPersDetailsByIp(String IPAddress) {

        SPersDetail detail = null;
        try {
            String query = "from SPersDetail where ipAddress=:ipAddress";
            Map<String, Object> map = new HashMap<>();
            map.put("ipAddress", IPAddress);
            List list = access.queryWithParams(query, map);
            if (list != null && !list.isEmpty()) {
                detail = (SPersDetail) list.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return detail;
    }

    @Transactional
    public OrgnStructure getOrgnStructure(String orgnName) {
        OrgnStructure orgnStructure = null;
        try {
            String OrgnId_query = "FROM OrgnStructure WHERE name='" + orgnName + "'";
            Map<String, Object> emptyMap = new HashMap<>();
            List Orgn_list = access.queryWithParams(OrgnId_query, emptyMap);
            orgnStructure = (OrgnStructure) Orgn_list.get(0);
        } catch (Exception e) {
        }
        return orgnStructure;
    }

    @Transactional
    public Integer getRegisteredUserCountByOrgIdAndRole(String OrgnId, String role) {
        Integer usersCount = 0;
        try {
            String cRoleUserQuery = "SELECT DISTINCT USER_NAME  FROM RPRT_PERS_DETAILS WHERE  ORGN_ID =:ORGN_ID AND ROLE_ID =:ROLE_ID ";
            Map<String, Object> map = new HashMap<>();
            map.put("ORGN_ID", OrgnId);
            map.put("ROLE_ID", role);
            List list = access.sqlqueryWithParams(cRoleUserQuery, map);
            if (list != null && !list.isEmpty()) {
                usersCount = list.size();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return usersCount;
    }

    @Transactional
    public String getLangIdByLocale(String loacle) {
        String langCode = "";
        try {
            String[] locales = loacle.split("_");
            String query = "from BLanguage where countryCode=:countryCode and languageCode=:languageCode";
            Map<String, Object> map = new HashMap<>();
            map.put("countryCode", locales[1]);
            map.put("languageCode", locales[0]);
            List list = access.queryWithParams(query, map);
            if (list != null && !list.isEmpty()) {
                BLanguage bLanguage = (BLanguage) list.get(0);
                langCode = bLanguage.getId().getLanguageId();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return langCode;

    }

    public BCountry getCountry(String countryName) {
        BCountry bCountry = null;
        try {
            String countryCode = "";
            String stdCode = "";
            if (countryName != null && !"".equalsIgnoreCase(countryName)
                    && !"null".equalsIgnoreCase(countryName) && countryName.contains(":")) {
                String[] countryArr = countryName.split(":");
                countryCode = countryArr[0];
                stdCode = countryArr[1];
            
            String query = "from BCountry where countryCode=:countryCode and stdCode=:stdCode";
            Map<String, Object> countryMap = new HashMap<>();
            countryMap.put("countryCode", countryCode);
            countryMap.put("stdCode", stdCode);
            List countryList = access.queryWithParams(query, countryMap);
            if (countryList != null && !countryList.isEmpty()) {

                bCountry = (BCountry) countryList.get(0);
            }
            }else if(countryName.length() == 2){
            	String query = "from BCountry where COUNTRY_CODE=:countryName";
                Map<String, Object> countryMap = new HashMap<>();
                countryMap.put("countryName", countryName);
                List countryList = access.queryWithParams(query, countryMap);
                if (countryList != null && !countryList.isEmpty()) {

                    bCountry = (BCountry) countryList.get(0);
                }
            }
            
            else
            {
            	String query = "from BCountry where description=:countryName";
                Map<String, Object> countryMap = new HashMap<>();
                countryMap.put("countryName", countryName);
                List countryList = access.queryWithParams(query, countryMap);
                if (countryList != null && !countryList.isEmpty()) {

                    bCountry = (BCountry) countryList.get(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bCountry;
    }

    @Transactional
    public Integer getUserCountByOrgIdAndRole(String orgnId, String role) {
        Integer usersCount = 0;
        try {
            String cRoleUserQuery = "select noOfUsers from CRoleUsers where id.roleId =:roleId and id.orgnId =:orgnId";
            Map<String, Object> map = new HashMap<>();
            map.put("orgnId", orgnId);
            map.put("roleId", role);
            List list = access.queryWithParams(cRoleUserQuery, map);
            if (list != null && !list.isEmpty()) {
                usersCount = ((BigInteger) list.get(0)).intValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return usersCount;
    }

    @Transactional
    public JSONObject passwordauthorisation(HttpServletRequest request) {
        JSONObject jsObject = null;
        JSONArray jSONArray = null;
        JSONObject jsonresult = new JSONObject();
        SAuthConfiguration sAuthConfiguration = null;
        SPersonnel sPersonnel = null;
        String orgnId = "";
        String persid = "";
        String userName = "";
        try {
            persid = request.getParameter("reset");
            if (persid != null) {
                sPersonnel = fetchSPersonnelByPersId(request, persid);
                orgnId = sPersonnel.getOrgnStructure().getOrgnId();
                userName = sPersonnel.getSPersDetail().getUserName();
                System.out.println("orgnId:::" + orgnId + "persid::::" + persid + ":::" + userName);
            } else {
                userName = request.getParameter("ssUsername");
                persid = cloudSheduleDAO.getPersId(userName, request);
                sPersonnel = fetchSPersonnelByPersId(request, persid);
                orgnId = sPersonnel.getOrgnStructure().getOrgnId();
            }
            String Queryauth = "FROM SAuthConfiguration where id.orgnId=:orgnId order by id.parameter";
            Map<String, Object> authMap = new HashMap<>();
            authMap.put("orgnId", orgnId);
            List listauth = access.queryWithParams(Queryauth, authMap);

            jSONArray = new JSONArray();
            if (listauth != null && !listauth.isEmpty()) {

                for (int i = 0; i < listauth.size(); i++) {

                    sAuthConfiguration = (SAuthConfiguration) listauth.get(i);
                    String passwordRegex = sAuthConfiguration.getProcessValue();
                    String passwordDesc = sAuthConfiguration.getDescription();
                    String parameter = sAuthConfiguration.getId().getParameter();

                    jsObject = new JSONObject();
                    jsObject.put("passwordRegex", passwordRegex);
                    jsObject.put("passwordDesc", passwordDesc);
                    jsObject.put("parameter", parameter);

                    jSONArray.add(jsObject);
                }
                jsonresult.put("passwordvalidation", jSONArray);
                jsonresult.put("userName", userName);

            }
        } catch (Exception e) {
        }

        return jsonresult;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public SPersonnel fetchSPersonnelByPersId(HttpServletRequest request, String persId) {
        SPersonnel sPersonnel = null;
        try {

            String Query = "FROM SPersonnel where SPersDetail.persId=:persId";
            Map<String, Object> personalmap = new HashMap<>();
            personalmap.put("persId", persId);
            List list = access.queryWithParams(Query, personalmap);
            if (!list.isEmpty()) {
                sPersonnel = new SPersonnel();
                sPersonnel = (SPersonnel) list.get(0);
            }
        } catch (Exception e) {
            logger.error("Exception in fetchSPersonnelByPersId", e);
        }
        return sPersonnel;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public String getBreadCrumbs(HttpServletRequest request, String componentId) {
        String result = "";
        JSONObject breadCrumbObj = new JSONObject();
        try {
            HttpSession session = request.getSession(false);
            String ssRole = (String) session.getAttribute("ssRole");
            if (!(ssRole != null && !"".equalsIgnoreCase(ssRole) && !"null".equalsIgnoreCase(ssRole))) {
                ssRole = "CLOUD";
            }
            String ssOrgId = (String) session.getAttribute("ssOrgId");
            if (componentId != null && !"".equals(componentId)) {

                String breadCrumbsQuery = "From com.pilog.mdm.pojo.DalRoleNvgn where id.componentId=:componentId and id.orgnId=:ssOrgId and id.roleId=:ssRole order by id.sequenceNo";
                Map breadCrumbsMap = new HashMap();
                breadCrumbsMap.put("ssRole", ssRole);
                breadCrumbsMap.put("ssOrgId", ssOrgId);
                breadCrumbsMap.put("componentId", componentId);
                List breadCrumbsList = access.queryWithParams(breadCrumbsQuery, breadCrumbsMap);
                JSONObject labelsObj = VisionUtills.getMultilingualObject(request);
                if (breadCrumbsList != null && !breadCrumbsList.isEmpty() && breadCrumbsList.size() > 0) {
                    result = "<ul>";
                    for (int i = 0; i < breadCrumbsList.size(); i++) {
                        DalRoleNvgn brdCrmbObj = (DalRoleNvgn) breadCrumbsList.get(i);
                        String href = brdCrmbObj.getId().getHref() != null ? brdCrmbObj.getId().getHref() : "";
                        String displayText = brdCrmbObj.getId().getDescription() != null ? brdCrmbObj.getId().getDescription() : "";
                        displayText = (String) new PilogUtilities().convertIntoMultilingualValue(labelsObj, displayText);
                        if (i != breadCrumbsList.size() - 1) {
                            result = result + "<li><a href='#' onclick=navigationMenuUrl(\'" + href + "\')>" + displayText + "</a></li>"
                                    //                            result = result + "<li><a href='" + href + "'>" + displayText + "</a></li>"
                                    + "<li class='uSeparator' ><span></span></li>";
                        } else {

                            result = result + "<li class='active' ><span>" + displayText + "</span></li>"
                                    + "<li class='uEndCap'> <span></span></li></ul>";
//                              breadCrumbObj.put("helpHref", ((brdCrmbObj.getId().getHelpHref() != null && !"".equalsIgnoreCase(brdCrmbObj.getId().getHelpHref())) ? brdCrmbObj.getId().getHelpHref() : "#"));
                        }

                    }
                    breadCrumbObj.put("breadCrumb", result);
                }
            }
            breadCrumbObj.put("helpHref", getHelpHref(componentId, ssRole, ssOrgId));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return breadCrumbObj.toJSONString();
    }

    @Transactional
    public String getHelpHref(String compId, String roleId, String orgnId) {
        String helpHref = "#";
        try {
            String helpHrefQuery = " from DalRoleHelp where id.compId =:compId and id.orgnId =:orgnId and id.roleId =:roleId";
            Map<String, Object> map = new HashMap<>();
            map.put("compId", compId);
            map.put("orgnId", orgnId);
            map.put("roleId", roleId);
            List helpHreflist = access.queryWithParams(helpHrefQuery, map);
            if (helpHreflist != null && !helpHreflist.isEmpty()) {
                DalRoleHelp dalRoleHelp = (DalRoleHelp) helpHreflist.get(0);
                if (dalRoleHelp != null) {
                    helpHref = dalRoleHelp.getId().getHelpHref() != null ? dalRoleHelp.getId().getHelpHref() : "#";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return helpHref;
    }

    @Transactional
    public JSONObject getUserData(HttpServletRequest request) {
        JSONObject profilejSONObject = new JSONObject();
//        BASE64Encoder encoder = new BASE64Encoder();
        String fileName = "";
        try {

            SPersDetail persDetail = cloudSheduleDAO.getEmailAddressByUsername(request);
            String persId = (String) persDetail.getPersId();
            SPersProfile persProfile = getValue(request, persId);
            String orgid = request.getParameter("ssOrgId");
            OrgnStructure orgnStructure = cloudSheduleDAO.getOrgnStructure(orgid);
            SPersnolisation persnolisation = getBlobContent(request, persId);
            if (persnolisation != null) {
                fileName = persnolisation.getId().getFileName();
                Blob b = persnolisation.getContent();
                Long blob_len = b.length();
                byte[] byteArray = new byte[blob_len.intValue()];

                b.getBinaryStream().read(byteArray);
//                String s = encoder.encode(byteArray);
                String s = Base64.getEncoder().encodeToString(byteArray);
                s = "data:image/png;base64," + s;
                profilejSONObject.put("blobString", s);
                profilejSONObject.put("file", fileName);
            } else {
                profilejSONObject.put("blobString", "images/no-image.jpg");

            }

            if (persDetail != null && persProfile != null) {

                profilejSONObject.put("Username", persDetail.getUserName());
                profilejSONObject.put("FirstName", persDetail.getFirstName());
                profilejSONObject.put("LastName", persDetail.getLastName());
                profilejSONObject.put("Address1", persDetail.getAddress1());
                profilejSONObject.put("Address2", persDetail.getAddress2());
                profilejSONObject.put("EmailId", persDetail.getEmail());
                profilejSONObject.put("MobileNo", persDetail.getMobile());
                profilejSONObject.put("PhoneNo", persDetail.getTelephone());
                profilejSONObject.put("DOB", persDetail.getDob());
                profilejSONObject.put("Country", persDetail.getBCountry().getDescription());
//                profilejSONObject.put("Country", persDetail.getBCountry().getDescription());
                profilejSONObject.put("Locale", persProfile.getId().getLocale());
                profilejSONObject.put("Role", persProfile.getId().getRoleId());
                profilejSONObject.put("ReportTo", persProfile.getReportTo());
                profilejSONObject.put("Plant", persProfile.getId().getPlant());
                profilejSONObject.put("Organization", orgnStructure.getName());
                profilejSONObject.put("Experience", persDetail.getExpSummary());
                profilejSONObject.put("PurposeReg", persDetail.getPurposeReg());

            }

        } catch (Exception e) {
        }
        return profilejSONObject;
    }

    @Transactional
    public SPersProfile getValue(HttpServletRequest request, String persId) {
        SPersProfile persProfile = null;
        try {
            String query = "from SPersProfile where id.persId=:persId";
            Map<String, Object> map = new HashMap<>();
            map.put("persId", persId);
            List list = access.queryWithParams(query, map);
            if (list != null && !list.isEmpty()) {
                persProfile = (SPersProfile) list.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return persProfile;
    }

    @Transactional
    public SPersnolisation getBlobContent(HttpServletRequest request, String persId) {

        SPersnolisation persnolisation = null;
        try {
            String query = "from SPersnolisation where id.persId=:persId ";
            Map<String, Object> profileMap = new HashMap<>();
            profileMap.put("persId", persId);
            List profileList = access.queryWithParams(query, profileMap);
            if (profileList != null && !profileList.isEmpty()) {

                persnolisation = (SPersnolisation) profileList.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return persnolisation;
    }

    @Transactional
    public String updateProfileData(HttpServletRequest request, JSONObject item) {
        String result = "";
        String persId = "";
        int recordsUpdated = 0;

        try {

            JSONArray ids = (JSONArray) item.get("ids");
            JSONArray values = (JSONArray) item.get("values");
            JSONObject profileItems = convertJsonOBj(ids, values);
            SPersDetail persDetail = cloudSheduleDAO.getEmailAddressByUsername(request);
            String profilequery = "From SPersDetail where persId=:persId";
            Map<String, Object> map = new HashMap<>();
            map.put("persId", (String) persDetail.getPersId());
            persId = (String) persDetail.getPersId();
            List list = access.queryWithParams(profilequery, map);
            if (!list.isEmpty()) {
                SPersDetail detail = (SPersDetail) list.get(0);
                detail.setEmail((String) profileItems.get("email_id"));
                detail.setAddress1((String) profileItems.get("country"));
                detail.setAddress2((String) profileItems.get("address"));
                detail.setMobile((String) profileItems.get("mobileno"));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date date = sdf.parse((String) profileItems.get("dob"));
                detail.setDob(date);
                access.update(detail);
                recordsUpdated++;
                result = "Updated Successfully";
            }
        } catch (Exception e) {
            e.printStackTrace();
            result = "Failed to update";
        }
        return result;
    }

    public JSONObject convertJsonOBj(JSONArray ids, JSONArray values) {
        JSONObject item = new JSONObject();
        try {
            for (int i = 0; i < values.size(); i++) {
                for (int j = 0; j < ids.size(); j++) {
                    if (i == j) {
                        item.put(ids.get(i), values.get(i));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return item;
    }

    @Transactional
    public BCountry getCountryCode(String countryName) {
        BCountry bCountry = null;
        try {
            String query = "from BCountry where Description=:Description";
            Map<String, Object> countryMap = new HashMap<>();
            countryMap.put("Description", countryName);
            List countryList = access.queryWithParams(query, countryMap);
            if (countryList != null && !countryList.isEmpty()) {
                bCountry = (BCountry) countryList.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bCountry;
    }

    @Transactional
    private void updatePic(String persId, JSONObject profileItems, JSONObject item, HttpServletRequest request) throws Exception {
        String filename = (String) item.get("oldPic");
        int recordupdated = 0;
        try {
            String newfilenamepath = (String) item.get("profilePic");
            String newfilename = newfilenamepath.substring(newfilenamepath.lastIndexOf("\\") + 1);
            byte[] filepatBytes = new PilogUtilities().loadFileAsBytesArray(new File(newfilenamepath));
            Blob blob = new javax.sql.rowset.serial.SerialBlob(filepatBytes);
            String checkquery = "From SPersnolisation where id.persId=:persId";
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("persId", persId);
            List checkList = access.queryWithParams(checkquery, map);
            SPersnolisation perssonnel = null;
            if (!checkList.isEmpty()) {
                String imageQuery = "UPDATE SPersnolisation set content=:content,id.fileName=:fileName WHERE id.persId=:persId";
                Map<String, Object> imageMap = new HashMap<>();
                imageMap.put("persId", persId);
                imageMap.put("content", blob);
                imageMap.put("fileName", newfilename);
                access.updateQuery(imageQuery, imageMap);
            } else {
                SPersnolisation persnolisation = new SPersnolisation();
                SPersnolisationId id = new SPersnolisationId();
                id.setPersId(persId);
                id.setFileName(newfilename);
                id.setType("P");
                persnolisation.setContent(blob);
                persnolisation.setCreateBy((String) request.getSession(false).getAttribute("ssUsername"));
                persnolisation.setEditBy((String) request.getSession(false).getAttribute("ssUsername"));
                persnolisation.setCreateDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                persnolisation.setEditDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                persnolisation.setId(id);
                access.save(persnolisation);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JSONObject changepassword(HttpServletRequest request) {
        JSONObject changePwdObj = new JSONObject();
        String persId = "";
        Date dob = null;
        String toEmailAdmin = "vision.admin@pilog.in";
        String orgnId = "", role = "", userName = "", emailText = "", resultdata = "", toEmail = "",loginFlag="";
        String emailSubject = "";

        SAuthorisation sAuthorisation = null;
        SPersonnel sPersonnel = null;
        request.setAttribute("ssLocale", "en_US");
        JSONObject labelObject = VisionUtills.getMultilingualObject(request);
        try {
            String context = request.getServletContext().getContextPath();
            String uri = request.getRequestURL().toString();
            HttpSession session = request.getSession(false);
            String link = uri.replaceAll("/changepassword", "");
            String header = link + "/images" + "/Email_Footer_New.jpg";
            String facebook = link + "/images" + "/fb.png";
            String twitter = link + "/images" + "/tw.png";
            String googleplus = link + "/images" + "/g.png";
            String linkedin = link + "/images" + "/ln.png";
            userName = request.getParameter("ssUsername");
            role = request.getParameter("ssRole");
            orgnId = request.getParameter("ssOrgId");
            loginFlag = request.getParameter("loginFlag");
            
            if (!(orgnId != null && !"".equalsIgnoreCase(orgnId) && !"null".equalsIgnoreCase(orgnId))) {
                orgnId = (String) request.getSession(false).getAttribute("ssOrgId");
            }
            if (!(loginFlag != null && !"".equalsIgnoreCase(loginFlag) && !"null".equalsIgnoreCase(loginFlag))) {
                loginFlag = "Y";
            }

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String old_password = request.getParameter("old_password");
            String password = request.getParameter("password");
            if (old_password != null && !"".equalsIgnoreCase(old_password)) {
                old_password = PilogEncryption.decryptText(old_password, "");
            }
            if (password != null && !"".equalsIgnoreCase(password)) {
                password = PilogEncryption.decryptText(password, "");
            }
            String Query = "FROM SPersDetail where userName=:userName";
            Map<String, Object> userMap = new HashMap<>();
            userMap.put("userName", userName);
            List list = access.queryWithParams(Query, userMap);
            if (list != null && !list.isEmpty()) {
                SPersDetail sPersDetail = (SPersDetail) list.get(0);
                if (sPersDetail != null) {
                    persId = sPersDetail.getPersId();
                    toEmail = sPersDetail.getEmail();
                    sAuthorisation = SAuthorisationByPersId(request, persId);
                    String passdb = sAuthorisation.getId().getPassPhrase();
                    RegistrationDTO registrationDTO = new RegistrationDTO();
                    dob = sPersDetail.getDob();
                    registrationDTO.setDate_of_birth(sdf.format(dob));
                    registrationDTO.setPassword(password);
                    registrationDTO.setUser_name(userName);
                    sPersonnel = fetchSPersonnelByPersId(request, persId);
                    if (!(orgnId != null && !"".equalsIgnoreCase(orgnId) && !"null".equalsIgnoreCase(orgnId))) {
                        orgnId = sPersonnel.getOrgnStructure().getOrgnId();
                    }

                    PilogEncryption genEnc = new PilogEncryption();

                    String passnew = genEnc.encrypt(password, registrationDTO.getUser_name(),
                            sPersDetail.getSPersonnel().getOrgnStructure().getOrgnId());
                    registrationDTO = new RegistrationDTO();
                    registrationDTO.setDate_of_birth(sdf.format(dob));
                    registrationDTO.setPassword(old_password);
                    registrationDTO.setUser_name(userName);
                    String passPhrold = genEnc.encrypt(old_password, registrationDTO.getUser_name(),
                            sPersDetail.getSPersonnel().getOrgnStructure().getOrgnId());
                    String user = sPersDetail.getFirstName() + " " + sPersDetail.getLastName();

                    String ntfnType = "CHANGEPASSWORD";
                    DalNtfnTemplate dalNtfnTemplate = cloudSheduleDAO.getmailTemplate(ntfnType);
                    emailSubject = dalNtfnTemplate.getSubject();
                    toEmailAdmin = dalNtfnTemplate.getEmailFrom();

                    Clob clobObject = (Clob) dalNtfnTemplate.getEmailTxt();
                    emailText = cloudSheduleDAO.clobToString(clobObject);
                    SAuthHistory authHistory = new SAuthHistory();

                    authHistory.setActivatedBy(sAuthorisation.getActivatedBy());
                    authHistory.setActivationDate(sAuthorisation.getActivationDate());
                    authHistory.setActivationMode(sAuthorisation.getActivationMode());
                    authHistory.setCreateDate(new Date());
                    authHistory.setCreateBy(sAuthorisation.getCreateBy());
                    authHistory.setEditBy(sAuthorisation.getEditBy());
                    authHistory.setEditDate(new Date());
                    authHistory.setOtp(sAuthorisation.getOtp());
                    authHistory.setAuditId(AuditIdGenerator.getAuditId("S_AUTH_HISTORY"));
                    authHistory.setReason(sAuthorisation.getReason());

                    SAuthHistoryId authHistoryId = new SAuthHistoryId();
                    authHistoryId.setSeq(new PilogUtilities().convertStringToBigInteger(numGen.get("MATERIAL", orgnId, "AUTH_SEQ")));
                    authHistoryId.setPersId(sAuthorisation.getId().getPersId());
                    authHistoryId.setPassPhrase(passnew);
                    authHistory.setId(authHistoryId);
                    if (emailText != null && !emailText.equalsIgnoreCase("")) {
                        emailText = emailText.replaceAll("<<-User:->>", user);
                        emailText = emailText.replaceAll("<<-Header:->>", header);
                        emailText = emailText.replaceAll("<<-Facebook:->>", facebook);
                        emailText = emailText.replaceAll("<<-Twitter:->>", twitter);
                        emailText = emailText.replaceAll("<<-Linkedin:->>", linkedin);
                        emailText = emailText.replaceAll("<<-Googleplus:->>", googleplus);
                        emailText = emailText.replaceAll("<<-active:->>", link);
                    }
                    String oldUserPassword = newuserPassword(persId);
                    String passwordFlag = "";
                    passwordFlag = (String) sPersonnel.getPasswordFlag();  
                    if (passwordFlag != null && !"".equalsIgnoreCase(passwordFlag) && ("R".equalsIgnoreCase(passwordFlag) || "N".equalsIgnoreCase(passwordFlag) || "N".equalsIgnoreCase(loginFlag))) {
                        sPersonnel.setPasswordFlag("U");
                        access.saveObj(authHistory);
                        access.saveObj(sPersonnel);
                        resultdata = cloudSheduleDAO.insertMailTemplete(emailText, orgnId, emailSubject, toEmail, userName, persId, "E");
                        passwordforgot(request, passnew, persId);
                        changePwdObj.put("messageFlag", true);
                        changePwdObj.put("message", new PilogUtilities().convertIntoMultilingualValue(labelObject, "Your password has been sucessfully updated"));
                    } else if (oldUserPassword != null && !"".equalsIgnoreCase(oldUserPassword) && oldUserPassword.equalsIgnoreCase(passPhrold)) {
                        if (!isMatchLast3Passwords(persId, passnew)) {
                            access.saveObj(authHistory);
                            resultdata = cloudSheduleDAO.insertMailTemplete(emailText, orgnId, emailSubject, toEmail, userName, persId, "E");
                            passwordforgot(request, passnew, persId);
                            changePwdObj.put("messageFlag", true);
                            changePwdObj.put("message", new PilogUtilities().convertIntoMultilingualValue(labelObject, "Your password has been sucessfully updated"));
                        } else if (!passPhrold.equals(passdb)) {
                            changePwdObj.put("messageFlag", false);
                            changePwdObj.put("message",
                                    new PilogUtilities().convertIntoMultilingualValue(labelObject,
                                            "Entered Old Password is incorrect"));
                        } else {
                            changePwdObj.put("messageFlag", false);
                            changePwdObj.put("message",
                                    new PilogUtilities().convertIntoMultilingualValue(labelObject,
                                            "Please choose a password that you haven't used before"));
                        }
                    } else {
                        changePwdObj.put("messageFlag", false);
                        changePwdObj.put("message",
                                new PilogUtilities().convertIntoMultilingualValue(labelObject,
                                        "Please enter exact Old Password"));
                    }
                }
            }
        } catch (Exception e) {
            changePwdObj.put("messageFlag", false);
            changePwdObj.put("message",
                    new PilogUtilities().convertIntoMultilingualValue(labelObject,
                            "Failed To Update"));
        }
        return changePwdObj;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public SAuthorisation SAuthorisationByPersId(HttpServletRequest request, String persId) {
        SAuthorisation sAuthorisation = null;
        try {
            String Query = "FROM SAuthorisation where id.persId=:persId";
            Map<String, Object> personalmap = new HashMap<>();
            personalmap.put("persId", persId);
            List list = access.queryWithParams(Query, personalmap);
            if (!list.isEmpty()) {
                sAuthorisation = new SAuthorisation();
                sAuthorisation = (SAuthorisation) list.get(0);
            }
        } catch (Exception e) {
            logger.error("Exception in SAuthorisationByPersId", e);
        }
        return sAuthorisation;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public String newuserPassword(String persId) {
        String result = "";
        String query = "SELECT PASS_PHRASE FROM S_AUTHORISATION WHERE PERS_ID=:persId";
        Map<String, Object> authHistoryMap = new HashMap<>();
        authHistoryMap.put("persId", persId);
        try {
            List authHistoryList = access.sqlqueryWithParams(query, authHistoryMap);
            if (authHistoryList != null && !authHistoryList.isEmpty()) {
                result = (String) authHistoryList.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Transactional
    public boolean isMatchLast3Passwords(String persId, String passPhrase) {
        boolean isMatched = false;
        try {
            Integer last3Pawds = 3;
            BApplProperties matchProp = (BApplProperties) loginHandlerDAO.getApplProperties("LAST_3_PASSWORDS");
            if (matchProp != null && matchProp.getId().getProcessValue() != null && !"".equalsIgnoreCase(matchProp.getId().getProcessValue())) {
                last3Pawds = Integer.parseInt(matchProp.getId().getProcessValue());
            }
            String query = " from SAuthHistory where id.persId =:persId  order by id.seq desc";
            Map<String, Object> authHistoryMap = new HashMap<>();
            authHistoryMap.put("persId", persId);
            List<SAuthHistory> authHistoryList = access.queryWithMaxLimit(query, authHistoryMap, last3Pawds);
            if (authHistoryList != null && !authHistoryList.isEmpty()) {
                authHistoryList = authHistoryList.stream().filter(sAuthHistory -> (sAuthHistory.getId().getPassPhrase() != null && !"".equalsIgnoreCase(sAuthHistory.getId().getPassPhrase())
                        && sAuthHistory.getId().getPassPhrase().equals(passPhrase))).distinct().collect(Collectors.toList());
                if (authHistoryList != null && !authHistoryList.isEmpty()) {
                    isMatched = true;
                } else {
                    isMatched = false;
                }
            } else {
                isMatched = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isMatched;
    }

    public void passwordforgot(HttpServletRequest request, String passPhrase, String persId) {
        try {
            String query = "Update SAuthorisation set id.passPhrase=:passPhrase where id.persId=:persId";
            Map<String, Object> map = new HashMap<>();
            map.put("passPhrase", passPhrase);
            map.put("persId", persId);
            access.updateQuery(query, map);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exception in resetpassword", e);
        }
    }

    public String checkUserNameAvailable(String userName) {
        String resultMsg = "";
        try {
            String checkUserQuery = "SELECT USER_NAME FROM S_PERS_DETAIL WHERE USER_NAME=:USER_NAME";
            Map checkUsermMap = new HashMap<>();
            checkUsermMap.put("USER_NAME", userName);
            int count = access.executeUpdateSQL(checkUserQuery, checkUsermMap);
            if (count > 0) {
                resultMsg = "User already exists.";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultMsg;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public String forgotpassword(HttpServletRequest request) {

        String result = "";
        String resultdata = "";
        String toEmailAdmin = "";
        String orgnId = "";
        String role = "";
        String userName = "";
        String persId = "";
        String emailText = "";
        String emailSubject = "";
        String sendEmailResult = "";

        try {

            String context = request.getServletContext().getContextPath();
            //System.out.println("context::::::" + context);
            String uri = request.getRequestURL().toString();
            String link = uri.replaceAll("/forgotpassword", "");
            String userId = request.getParameter("user_id");
            String email = request.getParameter("email_id");
            String contactNumber = request.getParameter("contact_number");

            //System.out.println("link::::::" + link);
            String header = link + "/images" + "/Email_Footer_New.jpg";
            String facebook = link + "/images" + "/fb.png";
            String twitter = link + "/images" + "/tw.png";
            String googleplus = link + "/images" + "/g.png";
            String linkedin = link + "/images" + "/ln.png";

            //System.out.println("header::::::" + header);
            String Query = "FROM SPersDetail where upper(email)=:email and upper(userName) =:userName and mobile=:mobile";
            Map<String, Object> userMap = new HashMap<>();
            userMap.put("email", email.toUpperCase());
            userMap.put("userName", userId.toUpperCase());
            userMap.put("mobile", contactNumber);
            System.out.println("userMap::::" + userMap);
            List list = access.queryWithParams(Query, userMap);

            if (list != null && !list.isEmpty()) {
                SPersDetail sPersDetail = (SPersDetail) list.get(0);
                persId = sPersDetail.getPersId();
                userName = sPersDetail.getUserName();
                String active = link + "/resetPass?value=2&reset=" + persId;
                //System.out.println("active link::::" + active);
                SPersDetail persDetail = cloudSheduleDAO.getDetailsOfUser(persId);//
                SPersProfile sPersProfile = cloudSheduleDAO.getRole(persId);
                role = sPersProfile.getId().getRoleId();
                orgnId = persDetail.getSPersonnel().getOrgnStructure().getOrgnId();

                String user = sPersDetail.getFirstName() + " " + sPersDetail.getLastName();
                //System.out.println("orgnId" + orgnId);
                String ntfnType = "FORGOTPASSWORD";
                DalNtfnTemplate dalNtfnTemplate = cloudSheduleDAO.getmailTemplate(ntfnType);
                emailSubject = dalNtfnTemplate.getSubject();
                toEmailAdmin = mailUsername;
                //toEmailAdmin = dalNtfnTemplate.getEmailFrom();

                Clob clobObject = (Clob) dalNtfnTemplate.getEmailTxt();
                emailText = cloudSheduleDAO.clobToString(clobObject);

                if (emailText != null && !emailText.equalsIgnoreCase("")) {
                    emailText = emailText.replaceAll("<<-User:->>", user);
                    emailText = emailText.replaceAll("<<-Header:->>", header);
                    emailText = emailText.replaceAll("<<-Facebook:->>", facebook);
                    emailText = emailText.replaceAll("<<-Twitter:->>", twitter);
                    emailText = emailText.replaceAll("<<-Linkedin:->>", linkedin);
                    emailText = emailText.replaceAll("<<-Googleplus:->>", googleplus);
                    emailText = emailText.replaceAll("<<-active:->>", active);
                }

                resultdata = cloudSheduleDAO.insertMailTemplete(emailText, orgnId, emailSubject, email, userName, persId, "E");
                sendEmailResult = cloudSheduleDAO.sendEmails(emailText, emailSubject, email, userName);
                if (sendEmailResult != "" && sendEmailResult.equalsIgnoreCase("success")) {
                    String nextline = System.getProperty("line.separator");
                    result = "An email was sent to " + email + " To reset your password.";
                } else {
                    result = "We are unable to send mail to " + email + "";
                }
                String nextline = System.getProperty("line.separator");
                result = "An email was sent to " + email + " To reset your password.";
//                        + "" + nextline + "Please make sure " + toEmailAdmin + "  is added to your safe senders list to stop this and future emails from us being stopped by spam filters";
            } else {
                result = "Entered User Id / Email Id / contact number doesn't match.";
            }

            //  result = "An email was sent to " + email + " To reset your password. " + nextline + "Please make sure " + toEmailAdmin + "  is added to your safe senders list to stop this and future emails from us being stopped by spam filters";
            //System.out.println("forgotpassword*************" + result);
            //true--> Not Exist
            //False--> Exist
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(":::::::Unable to Insert Record into DAL_MAIL_ADDRESS:::::");
            result = "Unable to send the mail.";
        }
        return result;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JSONObject getPersDetails(HttpServletRequest request, String username) {
        JSONObject userDetails = new JSONObject();

        try {

            if (username != null && !"".equalsIgnoreCase(username) && !"null".equalsIgnoreCase(username)) {
                String query = "SELECT PERS_ID, FIRST_NAME, LAST_NAME, EMAIL, MOBILE,ORGN_NAME,ORGN_ID FROM USER_MANAGEMENT_PERS_DETAILS WHERE USER_NAME = '" + username + "'";
                System.out.println("getPersDetails query is::::" + query);
                List userdetailsList = access.sqlqueryWithParamsLimit(query, Collections.EMPTY_MAP, 1, 0);
                System.out.println("userdetailsList list is::::" + userdetailsList);
                if (userdetailsList != null && !userdetailsList.isEmpty()) {
                    Object[] userdetailsArray = (Object[]) userdetailsList.get(0);
                    userDetails.put("persid", new RAW(userdetailsArray[0]).stringValue());
                    userDetails.put("firstname", userdetailsArray[1]);
                    userDetails.put("lastname", userdetailsArray[2]);
                    userDetails.put("email", userdetailsArray[3]);
                    userDetails.put("mobile", userdetailsArray[4]);
                    userDetails.put("orgnname", userdetailsArray[5]);
                    userDetails.put("orgnid", new RAW(userdetailsArray[6]).stringValue());
                    userDetails.put("flag", "Y");
                } else {
                    userDetails.put("flag", "N");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return userDetails;
    }

    @Transactional
    public List getLovFeedbackCat(HttpServletRequest request, String defaultOrgnId, String lovCat) {

        List lovfeedbackCatList = new ArrayList();

        try {
            HttpSession httpSession = request.getSession(false);
            String ssOrgnId = (String) httpSession.getAttribute("ssOrgId");
            if (!(ssOrgnId != null && !"".equalsIgnoreCase(ssOrgnId) && !"null".equalsIgnoreCase(ssOrgnId))) {
                ssOrgnId = defaultOrgnId;
            }

            String query = "SELECT DISPLAY,PROCESS_VALUE FROM DAL_DLOV"
                    + " WHERE ORGN_ID='" + ssOrgnId + "' and DLOV_NAME='" + lovCat + "'";
            lovfeedbackCatList = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return lovfeedbackCatList;
    }

    @Transactional
    public String getErrorMessage(int respCode) {
        String message = "";
        try {
            String query = "SELECT PROCESS_VALUE, HEADER FROM B_APPL_PROPERTIES WHERE KEYNAME ='" + respCode + "'";
            Map<String, Object> selectMap = new HashMap<>();
            // selectMap.put("KEYNAME", String.valueOf(respCode));
            List applList = access.sqlqueryWithParams(query, selectMap);
            if (applList != null && !applList.isEmpty()) {
                Object[] applDataArray = (Object[]) applList.get(0);
                if (applDataArray != null) {
                    message = applDataArray[0] + "(" + applDataArray[1] + ")";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    //onkar365
    @Transactional
    public JSONObject sendActivationMail(HttpServletRequest request, RegistrationDTO registrationDTO) {
        JSONObject resultObj = new JSONObject();
        String userName = "";
        String orgnId = "";
        String toEmail = "";
        String persId = "";
        String emailSubject = "";
        String emailText = "";
        String sendEmailResult = "";
        String result = "";
        String role = "";
        String email = "";
        try {
            String uril = request.getParameter("pilogCloudUrl");
            String context = request.getServletContext().getContextPath();
            String uri = request.getRequestURL().toString();
            HttpSession session = request.getSession(false);
            int port = request.getServerPort();
            String link = request.getRemoteAddr();//24322 changed to ip link
            link = "http://" + link + ":" + port + context; //24322 http impt
            String header = link + "/images" + "/Email_Footer_New.jpg";
            String facebook = link + "/images" + "/fb.png";
            String twitter = link + "/images" + "/tw.png";
            String googleplus = link + "/images" + "/g.png";
            String linkedin = link + "/images" + "/ln.png";
            userName = registrationDTO.getEmail_id();
            role = request.getParameter("ssRole");
            String uname = "";//16322
            uname = request.getParameter("userName");
            List list = new ArrayList();
            if (registrationDTO.getEmail_id() != null) {
                email = registrationDTO.getEmail_id().toUpperCase();//7322 uppercase coz all emails in DB are in UpperCase
                String Query = "FROM SPersDetail where email=:email"; //7322 changed username to email
                Map<String, Object> userMap = new HashMap<>();
                userMap.put("email", email);
                list = access.queryWithParams(Query, userMap);
            }
            if (uname != null) {
//                uname = request.getParameter("un");
                uname = new String(Base64.getDecoder().decode(uname.getBytes()));
                String query = "from SPersDetail where USER_NAME=:USER_NAME";
                Map<String, Object> map = new HashMap<>();
                map.put("USER_NAME", uname.toUpperCase());
                list = access.queryWithParams(query, map);
                SPersDetail persDetail = (SPersDetail) list.get(0);
                email = persDetail.getEmail();
            }
            BApplProperties errorMessageProps = (BApplProperties) loginHandlerDAO.getApplProperties("DEFAULT_ORGN_ID");
            if (errorMessageProps != null) {
                orgnId = errorMessageProps.getId().getProcessValue();
            }
            String Query = "FROM SPersDetail where userName=:userName";
            Map<String, Object> userMap = new HashMap<>();
            userMap.put("userName", uname);
            List userList = access.queryWithParams(Query, userMap);
            if (userList != null && !userList.isEmpty()) {
                SPersDetail sPersDetail = (SPersDetail) userList.get(0);
                if (sPersDetail != null) {
                    toEmail = sPersDetail.getEmail();
                    persId = sPersDetail.getPersId();
                    String ntfnType = "USER_ACTIVATION_LINK";
                    DalNtfnTemplate dalNtfnTemplate = cloudSheduleDAO.getmailTemplate(ntfnType);
                    emailSubject = dalNtfnTemplate.getSubject();
                    String toEmailAdmin = dalNtfnTemplate.getEmailFrom();
                    String userEmail = Base64.getEncoder().encodeToString(persId.getBytes());
                    Clob clobObject = (Clob) dalNtfnTemplate.getEmailTxt();
                    emailText = cloudSheduleDAO.clobToString(clobObject);
                    if (emailText != null && !emailText.equalsIgnoreCase("")) {
                        emailText = emailText.replaceAll("<<-User:->>", userName);
                        emailText = emailText.replaceAll("<<-Header:->>", header);
                        emailText = emailText.replaceAll("<<-Facebook:->>", facebook);
                        emailText = emailText.replaceAll("<<-Twitter:->>", twitter);
                        emailText = emailText.replaceAll("<<-Linkedin:->>", linkedin);
                        emailText = emailText.replaceAll("<<-Googleplus:->>", googleplus);
                        emailText = emailText.replaceAll("<<-persId:->>", persId);
                        emailText = emailText.replaceAll("<<-orgnId:->>", orgnId);
                        emailText = emailText.replaceAll("<<-active:->>", link + "/activateUser");
                        emailText = emailText.replaceAll("<<-userEmail:->>", userEmail);

                    }
//                    String sendMailNotificationForCourseBooking = cloudSheduleDAO.insertMailTemplete(emailText, orgnId, emailSubject, toEmail, uname, persId, "E");
                    sendEmailResult = cloudSheduleDAO.sendEmails(emailText, emailSubject, toEmail, uname);
                    if (sendEmailResult != "" && sendEmailResult.equalsIgnoreCase("success")) {
                        String nextline = System.getProperty("line.separator");
                        result = "An email was sent to " + toEmail + "";
                        resultObj.put("emailStatus", sendEmailResult);
                        resultObj.put("message", result);
                        resultObj.put("messageFlag", true);

                    } else {
                        result = "We are unable to send mail to " + toEmail + "";
                        resultObj.put("message", result);
                        resultObj.put("messageFlag", false);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @Transactional
    public JSONObject updateUserStatus(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        String status = "";
        String persId = "";
        String orgnId = "";
        try {

            String encEmail = request.getParameter("id");//14322 email to id
            String uname = request.getParameter("un");
            String email = "";
            if (uname != null) {//16322
                uname = new String(Base64.getDecoder().decode(uname.getBytes()));
                String query = "from SPersDetail where USER_NAME=:USER_NAME";
                Map<String, Object> map = new HashMap<>();
                map.put("USER_NAME", uname.toUpperCase());
                List list = access.queryWithParams(query, map);
                SPersDetail persDetail = (SPersDetail) list.get(0);
                persId = persDetail.getPersId();
            } else {
                //15322 Decrypting the id
                email = new String(Base64.getDecoder().decode(encEmail.getBytes()));
            }
            String selectQuery = "SELECT STATUS FROM S_PERSONNEL "
                    + " WHERE PERS_ID=:PERS_ID AND  ORGN_ID=:ORGN_ID";
            Map<String, Object> selectQueryMap = new HashMap();
            if (uname == null || uname.equalsIgnoreCase("")) {//16322
                selectQueryMap.put("PERS_ID", email);//email is the new persID they wont see that coming!  //10322 work first remove id from DB then check
            } else {
                selectQueryMap.put("PERS_ID", persId);
            }
            BApplProperties errorMessageProps = (BApplProperties) loginHandlerDAO.getApplProperties("DEFAULT_ORGN_ID");
            if (errorMessageProps != null) {
                orgnId = errorMessageProps.getId().getProcessValue();
            }
            selectQueryMap.put("ORGN_ID", orgnId);
            List resultList = access.sqlqueryWithParams(selectQuery, selectQueryMap);
            if (resultList != null && !resultList.isEmpty() && resultList.size() > 0) {
                System.out.println("resultList" + resultList.get(0));
                status = (String) resultList.get(0);
            }
            //check useractivatd or not

            if (status != null && !"".equalsIgnoreCase(status) && "ACTIVE".equalsIgnoreCase(status)) {
                resultObj.put("message", "User already Activated, Please click on Sign In.");
                resultObj.put("status", true);
            } else {
                String updateQuery = " UPDATE S_PERSONNEL SET STATUS='ACTIVE' "
                        + " WHERE PERS_ID=:PERS_ID AND  ORGN_ID=:ORGN_ID";
                Map<String, Object> updateQueryMap = new HashMap();
                if (uname == null || uname.equalsIgnoreCase("")) {//25322 added here
                    updateQueryMap.put("PERS_ID", email);//email is the new persID they wont see that coming!  //10322 work first remove id from DB then check
                } else {
                    updateQueryMap.put("PERS_ID", persId);
                }
                updateQueryMap.put("ORGN_ID", orgnId);
                int count = access.executeUpdateSQL(updateQuery, updateQueryMap);
                if (count > 0) {

                    resultObj.put("message", "User Activated succesfully.");
                    resultObj.put("status", true);

                } else {

                    resultObj.put("message", "User Activation failed.");
                    resultObj.put("status", false);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    //onkar365
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JSONObject registerValidation(HttpServletRequest request, JSONObject basicData) {
        JSONObject resultObj = new JSONObject();
        try {
            JSONObject labelsObj = VisionUtills.getMultilingualObject(request);
            String query = "";
            Map<String, Object> map = new HashMap<>();
            String registerValidateColumn = (String) basicData.get("registerValidateColumn");

            if (registerValidateColumn != null && !"".equalsIgnoreCase(registerValidateColumn)) {
                String[] registerValidateColumnArray = registerValidateColumn.split("[.]");
                if (registerValidateColumnArray != null && registerValidateColumnArray.length == 2) {

                    if (registerValidateColumnArray[1].trim() != null) {
                        query += "SELECT " + registerValidateColumnArray[1].trim() + " FROM " + registerValidateColumnArray[0].trim() + " WHERE " + registerValidateColumnArray[1].trim() + " =:" + registerValidateColumnArray[1].trim();
                        map.put(registerValidateColumnArray[1].trim(), basicData.get(registerValidateColumnArray[1].trim()));

//                        if (basicData.get("SENDER_MAIL") != null) {
//                            if (query != null && !"".equalsIgnoreCase(query)) {
//                                query += " OR SENDER_MAIL =:SENDER_MAIL";
//                                map.put("SENDER_MAIL", basicData.get("SENDER_MAIL"));
//                            }
//                        }
                    }
                } else {
                    System.err.println("*********** registerValidateColumn Not Configured ************");
                }
            } else {
                System.err.println("*********** registerValidateColumn Not Configured ************");
            }
            if (query != null && !"".equalsIgnoreCase(query)) {

                System.err.println("query::::" + query);
                System.err.println("map::::" + map);
                List validationList = access.sqlqueryWithParams(query, map);
                if (validationList != null && !validationList.isEmpty()) {
                    resultObj.put("validateFlag", false);
                    resultObj.put("message", new PilogUtilities().convertIntoMultilingualValue(labelsObj, basicData.get("REG_CONF_MESG")));
                } else {

                    if (basicData.get("SENDER_MAIL") != null) {
                        if (query != null && !"".equalsIgnoreCase(query)) {
                            query = query.substring(0, query.indexOf(" WHERE "));
                            query += " WHERE SENDER_MAIL =:SENDER_MAIL";
                            map.clear();
                            map.put("SENDER_MAIL", basicData.get("SENDER_MAIL"));
                            List mailValidationList = access.sqlqueryWithParams(query, map);
                            if (mailValidationList != null && !mailValidationList.isEmpty()) {
                                resultObj.put("validateFlag", false);
                                resultObj.put("message", new PilogUtilities().convertIntoMultilingualValue(labelsObj, "The Vendor/Buyer Email Id Already exist.Would you like to Process the Request") + ".");
                            } else {
                                resultObj.put("validateFlag", true);
                                resultObj.put("message", new PilogUtilities().convertIntoMultilingualValue(labelsObj, basicData.get("REG_CONF_MESG")));
                            }
                        }
                    } else {

                        resultObj.put("validateFlag", true);
                        resultObj.put("message", new PilogUtilities().convertIntoMultilingualValue(labelsObj, basicData.get("REG_CONF_MESG")));
                    }

                }

            } else {
                resultObj.put("validateFlag", true);
                resultObj.put("message", new PilogUtilities().convertIntoMultilingualValue(labelsObj, basicData.get("REG_CONF_MESG")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @Transactional
    public JSONObject checkOtp(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        String email_otp = (String) request.getSession(false).getAttribute("email_otp");

        String otpTextEntered = request.getParameter("otpValue");
        if (otpTextEntered.equals(email_otp)) {
            resultObj.put("check", true);
            request.getSession(false).removeAttribute("email_otp");
        } else {
            resultObj.put("check", false);
        }
        return resultObj;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public Object savePersDetail(RegistrationDTO registrationDTO, HttpServletRequest request) {
        SPersDetail sPersDetail = null;
        SPersProfile persProfile = null;
        SAuthorisation authorisation = null;
//        BPlant bPlant = null; //24222

        JSONObject resultObject = new JSONObject();
        try {
//            OrgnStructure orgnStructure = getOrgnStructure(registrationDTO.getOrgName());
            String role = registrationDTO.getRole();
            String subscriptionType = (String)request.getSession().getAttribute("Lgtype");
            subscriptionType = (subscriptionType !=null && !"".equalsIgnoreCase(subscriptionType) && !"null".equalsIgnoreCase(subscriptionType))?subscriptionType:(String)request.getSession().getAttribute("ssSubscriptionType");
            request.getSession(false).setAttribute("ssRole", role);

//            if (orgnStructure != null && getRegisteredUserCountByOrgIdAndRole(orgnStructure.getOrgnId(), role) <= getUserCountByOrgIdAndRole(orgnStructure.getOrgnId(), role)) {
//                String module = "";
//                if (role != null && !role.equalsIgnoreCase("") && role.startsWith("MM")) {
//                    module = "MaterialMaster";
//                } else if (role != null && !role.equalsIgnoreCase("") && role.startsWith("VM")) {
//                    module = "VendorMaster";
//                } else if (role != null && !role.equalsIgnoreCase("") && role.startsWith("CM")) {
//                    module = "CustomerMaster";
//                } else if (role != null && !role.equalsIgnoreCase("") && role.startsWith("SM")) {
//                    module = "ServiceMaster";
//                } else if (role != null && !role.equalsIgnoreCase("") && role.startsWith("CLOUD")) {
//                    module = "CLOUD";
//                } else {
//                    module = "Admin";
//                }
            Date ExpiryDate;
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTime(new Date());
            if (role != null && !role.equalsIgnoreCase("") && role.contains("CAAS")) {
                cal.add(Calendar.DATE, 7);
                ExpiryDate = cal.getTime();
            } else {
                ExpiryDate = getExpiryDate(registrationDTO.getOrgnId());
                Calendar expiryCal = Calendar.getInstance();
                expiryCal.setTime(ExpiryDate);
                ExpiryDate = expiryCal.getTime();
               
              }
            
            
            

//            LocalDate currentDate = LocalDate.now();
//            LocalDate ExpiryDate = currentDate.plusYears(2);
            
  
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String dob = registrationDTO.getDate_of_birth();
            sPersDetail = new SPersDetail();
//            sPersDetail.setFirstName("Test Name");//2222
            sPersDetail.setFirstName(registrationDTO.getFirst_name());//25222
//            sPersDetail.setMiddleName(registrationDTO.getMiddle_name());
            sPersDetail.setLastName(registrationDTO.getLast_name());
//                sPersDetail.setMiddleName(registrationDTO.getMiddle_name());
//                sPersDetail.setLastName(registrationDTO.getLast_name());
            sPersDetail.setAddress1("INDIA");
            sPersDetail.setAddress2("INDIA");
//            sPersDetail.setBCountry("INDIA");

//                sPersDetail.setAddress2(registrationDTO.getAddress2());
//                sPersDetail.setExpSummary(registrationDTO.getExperience_summary());
//                sPersDetail.setPurposeReg(registrationDTO.getPurposeofReg());
            sPersDetail.setBCountry(getCountry(registrationDTO.getCountry()));
            //sPersDetail.setBCountry(getCountry("IN:+91"));//ms365
//                if (!(dob != null && !"".equalsIgnoreCase(dob) && !"null".equalsIgnoreCase(dob))) {
//                    dob = "01-11-1991";
//                }
//                String userName = registrationDTO.getUser_name();
//                String userCheckMsg = checkUserNameAvailable(userName);
//                if (userCheckMsg != null && !"".equalsIgnoreCase(userCheckMsg) && !"null".equalsIgnoreCase(userCheckMsg)) {
//                    resultObject.put("Message", userCheckMsg);
//                    resultObject.put("MessageFlag", false);
//                    return resultObject;
//                }
                sPersDetail.setDob(sdf.parse(dob));
            //2222
//            String recipientEmail = (String) request.getSession(false).getAttribute("recipientEmail");

            sPersDetail.setEmail(registrationDTO.getEmail_id());
            //sPersDetail.setEmail(recipientEmail);

            sPersDetail.setMobile(registrationDTO.getMobile_number());
            //sPersDetail.setMobile("9999999999");//ms365

            sPersDetail.setTelephone("040 74589634");
//            sPersDetail.setUserName("Koyomi Araragi");
            sPersDetail.setUserName(registrationDTO.getUser_name());
            sPersDetail.setUserAuth(registrationDTO.getDefaultUserFlag());
            sPersDetail.setCreateBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
//            sPersDetail.setEditBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            sPersDetail.setEditBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            sPersDetail.setCreateDate(new Date());
            sPersDetail.setEditDate(new Date());
            sPersDetail.setExpiryDate(ExpiryDate);//24222
            //sPersDetail.setOrganisationName(registrationDTO.getUserDepartment());//24222
            sPersDetail.setAuditId(AuditIdGenerator.getAuditId("S_PERS_DETAIL"));
            sPersDetail.setPersId(AuditIdGenerator.genRandom32Hex());
//                String device_detailsString = new PilogUtilities().getDeviceDetails(request, macCommand);
//                JSONObject deviceDetails = (JSONObject) JSONValue.parse(device_detailsString);
//                sPersDetail.setIpAddress((String) deviceDetails.get("IPAddress"));

            persProfile = new SPersProfile();
            SPersProfileId persProfileId = new SPersProfileId();
            persProfile.setReportTo(registrationDTO.getReport_to());
            persProfile.setDefaultInd("Y");
            persProfileId.setPersId(sPersDetail.getPersId());//23222
            persProfileId.setInstance(registrationDTO.getInstance());
            persProfileId.setLocale("en_US");
            persProfileId.setPlant(registrationDTO.getPlant());
            persProfileId.setRegion("IN");
            persProfileId.setRoleId(registrationDTO.getRole());
            persProfile.setId(persProfileId);
            persProfile.setDomain("PRODUCT");
            persProfile.setProject("Y");
            persProfile.setCreateBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            persProfile.setEditBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            persProfile.setCreateDate(new Date());
            persProfile.setEditDate(new Date());
            persProfile.setSPersDetail(sPersDetail);
//            persProfile.setAuditId(sPersDetail.getAuditId());//23222
            persProfile.setAuditId(AuditIdGenerator.getAuditId("S_PERS_PROFILE"));//24222AuditIdGenerator.getAuditId("

            persProfile.setCreateBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());//23222
            persProfile.setEditBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());//23222
//            bPlant = new BPlant();//24222 disabled in toad as no getter setter method assigned once available can use
//            bPlant.setPlant(registrationDTO.getPlant());//24222
//            bPlant.setInstance(registrationDTO.getInstance());//24222

            //add desc orgnid creatby editby auditid createdate editdate in bplant
//            (BPlant)persProfile.setBPlant(registrationDTO.getPlant());//23222    WORK HERE
            Set<SPersProfile> persProfiles = new HashSet<>();
            persProfiles.add(persProfile);
            sPersDetail.setSPersProfiles(persProfiles);
            PilogEncryption genEnc = new PilogEncryption();
            authorisation = new SAuthorisation();
            SAuthorisationId authorisationId = new SAuthorisationId();
//
            authorisationId.setPassPhrase(genEnc.encrypt(registrationDTO.getPassword(), registrationDTO.getUser_name(), registrationDTO.getRole()));
//25222 passphase was getting overridden            authorisationId.setPassPhrase(genEnc.encrypt(registrationDTO.getPassword(), registrationDTO.getEmail_id(), registrationDTO.getRole()));//MS365
            authorisation.setId(authorisationId);
            authorisation.setActivationMode("MOBILE");
            authorisation.setOtp("123456");
            authorisation.setActivationDate(new Date());
            authorisation.setActivatedBy("ADMIN");
            authorisation.setCreateBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            authorisation.setEditBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            authorisation.setCreateDate(new Date());
            authorisation.setEditDate(new Date());
            authorisation.setSPersDetail(sPersDetail);
            authorisation.setAuditId(AuditIdGenerator.getAuditId("S_AUTHORISATION"));
            Set<SAuthorisation> authorisations = new HashSet<>();
//
            authorisations.add(authorisation);
//
            sPersDetail.setSAuthorisations(authorisations);

            access.save(sPersDetail);

            persProfileId.setPersId(sPersDetail.getPersId());
            persProfile.setId(persProfileId);
            access.save(persProfile);
            authorisationId.setPersId(sPersDetail.getPersId());
            authorisation.setId(authorisationId);
            access.save(authorisation);

            SPersnolisation persnolisation = new SPersnolisation();
            SPersnolisationId persnolisationId = new SPersnolisationId();
            persnolisation.setCreateBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            persnolisation.setEditBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            persnolisation.setCreateDate(new Date());
            persnolisation.setEditDate(new Date());
//            persnolisation.setAuditId(persProfile.getAuditId());24222
            persnolisation.setAuditId(AuditIdGenerator.getAuditId("S_PERSNOLISATION"));
            //registrationDTO.setFilepath("D:\\Murtuza");//24222
//
            if (registrationDTO.getFilepath() != null
                    && !registrationDTO.getFilepath().equalsIgnoreCase("")) {

                persnolisationId.setPersId(sPersDetail.getPersId());

                String filepath = registrationDTO.getFilepath();
                String filename = filepath.substring(filepath.lastIndexOf(File.separator) + 1);
                persnolisationId.setFileName(filename);
                persnolisationId.setType("P");
                persnolisation.setId(persnolisationId);
                persnolisation.setSPersDetail(sPersDetail);
                access.save(persnolisation);
            }
            //saving orgnStructure 24222
            OrgnStructure orgnStructure = getOrgnStructure(registrationDTO.getOrgName());
            SPersonnel personnel = new SPersonnel();
            personnel.setExpiryDate(ExpiryDate);
            personnel.setCreateBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            personnel.setEditBy(registrationDTO.getFirst_name() + " " + registrationDTO.getLast_name());
            personnel.setCreateDate(new Date());
            personnel.setEditDate(new Date());
            personnel.setSPersDetail(sPersDetail);
            personnel.setPersId(sPersDetail.getPersId());
            personnel.setPasswordFlag("A");
            personnel.setLoginAttempts(BigInteger.ZERO);
            personnel.setStatus("ACTIVE");//inactive to active for login 24222
            personnel.setAuditId(AuditIdGenerator.getAuditId("S_PERSONNEL"));
            personnel.setOrgnStructure(orgnStructure);
            access.save(personnel);
            String encUser = Base64.getEncoder().encodeToString(registrationDTO.getUser_name().getBytes());
            String url = request.getRequestURL().toString();
            url = url.replace("register", "activateUserThroughLink");
            String src = "<a class=\"redirectToActivateUser\" onclick=\"activationMailToUser('<<-un->>')\" style=\"\n"//22322
                    + "    font-size: 1.2rem;\n"
                    + "    color: #0b4a99;\n"
                    + "    font-weight: bolder;\n"
                    + "\">Activate Account</a><br>";
            src = src.replaceAll("<<-un->>", encUser);
            src = src.replaceAll("<<-url->>", url);

            resultObject.put("Message", "Registration Successfully Completed.");//15322
            resultObject.put("MessageFlag", true);
            resultObject.put("userName", registrationDTO.getUser_name());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exceptions In savePersDetail: " + e.getLocalizedMessage());
            resultObject.put("Message", e.getLocalizedMessage());
            resultObject.put("MessageFlag", false);
        }
        return resultObject;
    }

    @Transactional
    public JSONObject emailOTP(HttpServletRequest request) throws Exception {
        JSONObject resultObj = new JSONObject();
        int count = 0;
        try {
            DalMailConfig dalMailConfig = cloudSheduleDAO.getMailConfigurationByOrgnId();
            System.out.println("com.pilog.mdm.DAO.CloudRegistrationDAO.emailOTP()");

            String recipientEmail = request.getParameter("email");
            request.getSession(false).setAttribute("recipientEmail", recipientEmail);
            resultObj.put("recipientEmail", recipientEmail);
//        String emailQuery = "select email from S_PERS_DETAIL";
            String emailQuery = "SELECT COUNT(*) FROM S_PERS_DETAIL WHERE EMAIL='" + recipientEmail.toUpperCase() + "'";
            List verified_email_List = access.sqlqueryWithParams(emailQuery, Collections.EMPTY_MAP);
            if (verified_email_List != null && !verified_email_List.isEmpty()) {
                count = new PilogUtilities().convertIntoInteger(verified_email_List.get(0));
            }

            if (count == 0) {
                Properties properties = new Properties();
                properties.setProperty("mail.smtp.host", dalMailConfig.getSmtpHost());
                properties.put("mail.transport.protocol", dalMailConfig.getTransportProtocol());
                properties.put("mail.smtp.starttls.enable", dalMailConfig.getSmtpStarttlsEnable());
                properties.put("mail.smtp.auth", dalMailConfig.getSmtpAuth());
                properties.put("mail.smtp.port", dalMailConfig.getSmtpPort());
                Session session = Session.getInstance(properties,
                        new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(dalMailConfig.getUserName(), dalMailConfig.getPasword());
                    }
                });
//        OTPGENERATION
                String otpGen = RandomStringUtils.randomAlphanumeric(4).toUpperCase();
                request.getSession(false).setAttribute("email_otp", otpGen);
                System.out.println("com.pilog.mdm.DAO.CloudRegistrationDAO.emailOTP()" + otpGen);
                resultObj.put("otpGen", otpGen);
                resultObj.put("flag", true);
                String recievedOTP = request.getParameter("otpValue");
                try {
                    MimeMessage msg = new MimeMessage(session);
                    msg.setFrom(new InternetAddress(dalMailConfig.getUserName()));
                    msg.addRecipient(Message.RecipientType.TO, new InternetAddress(recipientEmail));
                    msg.setSubject("OTP Verification for Registration");
//            msg.setHeader("Your OTP for SIGNUP is :",otpGen);
                    msg.setText("Your OTP for registration is: " + otpGen + "");
                    Transport.send(msg);
                } catch (MessagingException mex) {
                    mex.printStackTrace();
                }
            } else {
                resultObj.put("message", "Email Already Exists, Try Other one");
                resultObj.put("flag", false);
            }

        } catch (Exception e) {
        }

        return resultObj;
    }

    
    

    @Transactional
    public JSONObject getProcessLoginAuth(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            String username = request.getParameter("userName");
            String processVal = "";
            String persId = "";
            String email = "";
            String status = "";
            String passwordFlag = "";
            String query = "SELECT USER_NAME, PASSWORD_FLAG,STATUS, EMAIL FROM S_PERS_DETAIL A, S_PERSONNEL B WHERE A.PERS_ID= B.PERS_ID AND A.USER_NAME =:USER_NAME";
            Map<String, Object> map = new HashMap<>();
            map.put("USER_NAME", username.toUpperCase());
            List resultList = access.sqlqueryWithParams(query, map);
            if (!(resultList != null && !resultList.isEmpty() && resultList.size() > 0)) {
                resultObj.put("message", "User doesn't Exists");
                resultObj.put("messageFlag", false);
            } else {
                if (resultList != null && !resultList.isEmpty() && resultList.size() > 0) {
                    System.out.println("resultList" + resultList.get(0));
                    Object[] dataArray = (Object[]) resultList.get(0);
                    passwordFlag = (String) dataArray[1];
                    status = (String) dataArray[2];
                    email = (String) dataArray[3];
                    String maskedEmail = maskString(email);
                    resultObj.put("email", maskedEmail);
                    resultObj.put("passwordFlag", passwordFlag);

                    if (status != null && !"".equalsIgnoreCase(status) && "INACTIVE".equalsIgnoreCase(status)) {
                        resultObj.put("message", "UserID is in Inactive Status, Please Contact your Administrator to activate it.");
                        resultObj.put("status", "INACTIVE");
                        resultObj.put("messageFlag", false);
                    }
                    if (passwordFlag != null && !"".equalsIgnoreCase(passwordFlag)
                            && ("R".equalsIgnoreCase(passwordFlag) || "N".equalsIgnoreCase(passwordFlag))) {
                        resultObj.put("messageFlag", false);
                    }
                }
            }

            String processQuery = "SELECT PROCESS_VALUE FROM B_APPL_PROPERTIES WHERE KEYNAME='LOGIN_2_AUTH'";
            List processList = access.sqlqueryWithParams(processQuery, Collections.EMPTY_MAP);
            if (processList != null && !processList.isEmpty()) {
                processVal = processList.get(0).toString();
            }
            String smallProcessValStr = processVal.toLowerCase();
            resultObj.put("processVal", smallProcessValStr);

            if (smallProcessValStr != null && smallProcessValStr.contains("otp") && smallProcessValStr.contains("captcha")) {
                // Both "otp" and "captcha" conditions are satisfied
                String captchaCode = "<div class='dxpCaptchaCodeChanges'>"
                        + "<div id=\"CaptchaImageCode\" class=\"CaptchaTxtField\">"
                        + "<canvas id=\"CapCode\" class=\"capcode\"></canvas></div>"
                        + "<div class='refreshIconDiv'><img src=\"images/refresh.png\" class=\"ReloadBtn\" onclick='CreateCaptcha()'></div>"
                        + "<div class=\"form-group form-primary\">"
                        + "<input type=\"text\" name=\"userCaptchaCode\" id=\"userCaptchaCode\" "
                        + "placeholder=\"Enter Captcha\" data-man=\"M\" class=\"form-control fill \" "
                        + "required=\"\" spellcheck=\"false\" data-ms-editor=\"true\" onkeypress=\"showorHideError('password')\">"
                        + "</div>"
                        + "</div>"
                        + "<div id=\"captchaErrorrText\"></div>";
                String OtpCode = "<div><div id=\"OtpEnterError\" style=\"color:red\"></div><button type=\"button\" id=\"securityVerifyOtp\" onclick=\"otpSend('" + username + "')\" class=\"btn btn-primary\">OTP</button></div>";
                resultObj.put("OtpCode", OtpCode);
                resultObj.put("captchaCode", captchaCode);
            } else if (smallProcessValStr != null && smallProcessValStr.contains("otp")) {
                // Only "otp" condition is satisfied
                String OtpCode = "<div><div id=\"OtpEnterError\" style=\"color:red\"></div><button type=\"button\" id=\"securityVerifyOtp\" onclick=\"otpSend('" + username + "')\" class=\"btn btn-primary\">OTP</button></div>";
                resultObj.put("OtpCode", OtpCode);
            } else if (smallProcessValStr != null && smallProcessValStr.contains("captcha")) {
                // Only "captcha" condition is satisfied
                String captchaCode = "<div class='dxpCaptchaCodeChanges'>"
                        + "<div id=\"CaptchaImageCode\" class=\"CaptchaTxtField\">"
                        + "<canvas id=\"CapCode\" class=\"capcode\"></canvas></div>"
                        + "<div class='refreshIconDiv'><img src=\"images/refresh.png\" class=\"ReloadBtn\" onclick='CreateCaptcha()'></div>"
                        + "<div class=\"form-group form-primary\">"
                        + "<input type=\"text\" name=\"userCaptchaCode\" id=\"userCaptchaCode\" "
                        + "placeholder=\"Enter Captcha\" data-man=\"M\" class=\"form-control fill \" "
                        + "required=\"\" spellcheck=\"false\" data-ms-editor=\"true\" onkeypress=\"showorHideError('password')\">"
                        + "</div>"
                        + "</div>"
                        + "<div id=\"captchaErrorrText\"></div>";
                resultObj.put("captchaCode", captchaCode);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }

    @Transactional
    public JSONObject getProcessLoginOtpAuth(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        try {
            String username = request.getParameter("userName");
            String beforeLoginFlag = request.getParameter("beforeLogin");
            String profileQuery = "SELECT EMAIL FROM S_PERS_DETAIL WHERE USER_NAME='" + username + "'";
            List usersList = access.sqlqueryWithParams(profileQuery, Collections.EMPTY_MAP);
            if (usersList != null && !usersList.isEmpty()) {
                String usersEmail = usersList.get(0).toString();
                String otp = new DecimalFormat("0000").format(new Random().nextInt(9999));
                Encoder encoder = Base64.getUrlEncoder();
                String EncOtp = encoder.encodeToString(otp.getBytes());
                resultObj.put("otp", EncOtp);
                String subject = "OTP";
                String Colunm = "Please Enter Otp";
                String strOtp ="Your OTP for registration is: "+otp+"";
                String userOtpSuccess = cloudSheduleDAO.sendEmails(strOtp, subject, usersEmail, Colunm);
                resultObj.put("status", userOtpSuccess);
                if (userOtpSuccess != null && !"".equalsIgnoreCase(userOtpSuccess) && "success".equalsIgnoreCase(userOtpSuccess)) {
                	 if (beforeLoginFlag != null && beforeLoginFlag != "" && beforeLoginFlag.equalsIgnoreCase("Y")) {
                         resultObj.put("status", userOtpSuccess);
                         String otpInput = "<div class=\"loginOtpStatusClass\" id=\"loginOtpStatus\"></div>"
                                 +"<div class=\"row m-0\">"
                                 +"<div class=\"col-md-4\"><label id=\"otpLabelId\" data-error=\"wrong\" data-success=\"right\" for=\"form34\">Enter Otp<sup style=\"color: red\">*</sup></label></div>"
                                 + "<div class=\"col-md-8 form-group form-primary\">"
                                 + "<div class=\"user-wrapper\"><input type=\"text\" name=\"loginOtpCode\" id=\"loginOtpCode\" "
                                 + "placeholder=\"Enter OTP\" data-man=\"M\" class=\"form-control fill CaptchaTxtField\" "
                                 + "required=\"\" spellcheck=\"false\" data-ms-editor=\"true\"/>"
                                 + "<button id='checkOtpAndEnableId' class='btn btn-primary' onclick='checkOtpAndEnableIp()'>Verify OTP</button></div>"
                                 + "</div></div>";
                         resultObj.put("otpInput", otpInput);

						} else {
							String otpInput = "<input type=\"text\" id=\"otpUserCode\" onchange=\"userEnterSecurity('"
									+ subject.toLowerCase() + "')\" class=\"CaptchaOtpField\" placeholder='Enter Otp'>"
									+ "<div class='otpTextsSuccess'><small id='otpTextsSuccess' class='otpTextsSuccess'>OTP has been sent to "
									+ usersEmail + "</small></div>"
									+ "<div class='otpTextsError'><small id='otpErrorrText' class='otpErrorrText'></small></div>"
									+ "<div class='timeSession'><span id='timerText' class='timerText'>Resending in: </span><span id='timeSession' class='timeSessiontest'></span></div>"
									+ "<div id='resenddiv'><button type=\"button\" id==\"resendOtp\" onclick=\"otpSend('"
									+ username + "')\" class=\"btn btn-primary\">Resend</button></div>";
							resultObj.put("otpInput", otpInput);
							String setOtp = String.valueOf(otp);
						}
                } else if (userOtpSuccess != null && !"".equalsIgnoreCase(userOtpSuccess) && "fail".equalsIgnoreCase(userOtpSuccess)) {
                    String otpInput = "<div><button type=\"button\" id==\"securityVerifyOtp\" onclick=\"otpSend('" + username + "')\" class=\"btn btn-primary\">Get OTP</button>"
                            + "<div class='otpTextsError'><small id='otpErrorrText' class='otpErrorrText'></small></div></div>";
                    resultObj.put("otpInput", otpInput);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultObj;
    }
    public static String maskString(String str) {
        int length = str.length();
        int maskLength = Math.min(length, 4); // You can adjust the number of characters to mask
        StringBuilder maskedChars = new StringBuilder();
        for (int i = 0; i < maskLength; i++) {
            maskedChars.append('*');
        }
        return maskedChars.toString() + str.substring(maskLength);
    }
    
    
    public JSONObject checkForRole(HttpServletRequest request) {
        JSONObject resultObj = new JSONObject();
        StringBuilder formData = new StringBuilder();
        try { 
        	String userName = (String) request.getSession(false).getAttribute("ssUsername");
        	String orgnId= (String)request.getSession(false).getAttribute("ssOrgId");
                    String roleId= (String)request.getSession(false).getAttribute("ssRole");
        	String fetchQuery = "SELECT PROJECT FROM S_PERS_PROFILE WHERE ROLE_ID=:ROLE_ID AND PERS_ID IN (SELECT PERS_ID FROM S_PERS_DETAIL WHERE USER_NAME=:USER_NAME)";
        	Map tempMap = new HashMap<>();
        	tempMap.put("USER_NAME", userName);
                    tempMap.put("ROLE_ID", roleId);
			List tempList = access.sqlqueryWithParams(fetchQuery ,tempMap);
			if (tempList != null && !tempList.isEmpty()) { 
				String userCreateFlag= (String) tempList.get(0);
				if(userCreateFlag !=null && !"".equalsIgnoreCase(userCreateFlag) && "Y".equalsIgnoreCase(userCreateFlag)) {
					resultObj.put("status",true);
					formData.append("<div id='formForUserRegisterId' class='formForUserRegisterClass'>"
							+ "<div class='formForUserRegisterEachRowCLS'>" 
							+ "<div><span style='color:red'>User Name</span></div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<input onblur=isUserAvailable() value='' data-label='User Name' data-regex='null' data-colname='USER_NAME' data-man='M' id='USER_NAME' init_value='null' type='text' />"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_USER_NAME_disp'></span>"
							+ "</div>"
							+ "<div>"
							+ "<span style='color:red'>First Name</span>"
							+ "</div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<input value='' data-label='First Name' data-regex='null' data-colname='FIRST_NAME' data-man='M' id='FIRST_NAME' init_value='null' type='text' />"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_FIRST_NAME_disp'></span>"
							+ "</div>"
							+ "<div>"
							+ "<span style='color:red'>Last Name</span>"
							+ "</div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<input value='' data-label='Last Name' data-regex='null' data-colname='LAST_NAME' data-man='M' id='LAST_NAME' init_value='null' type='text' />"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_LAST_NAME_disp'></span>"
							+ "</div>"
							+ "</div>"
							+ "<div class='formForUserRegisterEachRowCLS'>"
							+ "<div>"
							+ "<span >Telephone</span>"
							+ "</div>"
							+ "<div>"
							+ "<input value='' data-label='Telephone' data-regex='null' data-colname='TELEPHONE' data-man='M' id='TELEPHONE' init_value='null' type='text' />"
							+ "</div>"
							+ "<div>"
							+ "<span style='color:red'>Email</span>"
							+ "</div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<input onblur=\" return regexFunction(\" EMAIL\") \"  data-regex='^(NA|na|Na|nA)|([a-zA-Z0-9]{1}[a-zA-Z0-9_.-]{0,}([a-zA-Z0-9]{1})+@[a-zA-Z0-9]([a-zA-Z0-9.-]{0,}[a-zA-Z0-9])+\\\\.[a-zA-Z]{3})|([a-zA-Z0-9]{1}[a-zA-Z0-9_.-]{0,}([a-zA-Z0-9]{1})+@[a-zA-Z0-9]([a-zA-Z0-9.-]{0,}[a-zA-Z0-9])+\\\\.[a-zA-Z]{2})|([a-zA-Z0-9]{1}[a-zA-Z0-9_.-]{0,}([a-zA-Z0-9]{1})+@[a-zA-Z0-9]([a-zA-Z0-9.-]{0,}[a-zA-Z0-9])+\\\\.[a-zA-Z]{3}[.]{1}[a-zA-Z]{2})$'  value='' data-label='Email' data-regex='15' data-colname='EMAIL' data-man='M' id='EMAIL'init_value ='null' type='text'/>"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_EMAIL_disp'></span>"
							+ "</div>"
							+ "<div>"
							+ "<span style='color:red'>Role</span>"
							+ "</div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<div class='formForUserHasImgCLS'>"
							+ "<input disabled=disabled value='' data-label='Role' data-regex='null' data-colname='ROLE_ID' data-man='M' id='ROLE_ID'init_value ='null' type='text'/>"
							+ "<img class='prop_imgClass visionMasterDetailFormddw' src='images/search_icon_color_2.png'onclick=\"visionDropdown('DDW_ROLES_UM','','FORM-VIEW','MM_MASTER_UM_USER_DETAILS','ROLE_ID','')\" />"
							+ "</div>"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_ROLE_ID_disp'></span>"
							+ "</div>"
							+ "</div>"
							+ "<div class='formForUserRegisterEachRowCLS'>"
							+ "<div>"
							+ "<span style='color:red'>Status</span>"
							+ "</div>"
							
							+ "<div>"
							+ "<select data-regex='null' data-label='Status' id='STATUS' data-colname='STATUS'>"
							+ "<option value='ACTIVE'>ACTIVE</option>"
//							+ "<option value='INACTIVE'>INACTIVE</option>"
							+ "</select>"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_STATUS_disp'></span>"
							+ "</div>"
							+ "<div> <span style='color:red'>Country</span></div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<div class='formForUserHasImgCLS'>"
							+ "<input disabled=disabled value='' data-label='Country' data-regex='null' data-colname='COUNTRY' data-man='M' id='COUNTRY' init_value ='null' type='text'/>"
							+ "<img class='prop_imgClass visionMasterDetailFormddw' src='images/search_icon_color_2.png'onclick=\"visionDropdown('DDW_VM_COUNTRY','PLANT','FORM-VIEW','MM_MASTER_UM_USER_DETAILS','LAND1','')\" />"
							+ "</div>"
							+ "<span class='visionMasterDetailForm_err' id='COUNTRY_disp'></span></div>"
							
							+ "</div>" 
							+ "<div class='formForUserRegisterEachRowCLS'>"
							+ "<div><span style='color:red'>Dob</span></div>"
							+ "<div class='formForUserHasErrorMsgCLS'><input value='' data-label='Dob' data-regex='null' data-colname='DOB' data-man='M' id='DOB' init_value='null' type='date' />"
							+ "<span class='visionMasterDetailForm_err' id='DOB_disp'></span>"
							+ "</div>"
							+ "<div><span>Address 2</span></div>"
							+ "<div class='formForUserHasErrorMsgCLS'><input value='' data-label='Address 2' data-regex='null' data-colname='ADDRESS_2' data-man='O' id='FFUR_ADDRESS_2' init_value='null' type='text' />"
							+ "<span class='visionMasterDetailForm_err' id='ADDRESS_2_disp'></span></div>"
							+ "<div><span>Address 1</span></div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<input value='' data-label='Address 1' data-regex='null' data-colname='ADDRESS_1' data-man='O' id='FFUR_ADDRESS_1' init_value='null' type='text' />"
							+ "<span class='visionMasterDetailForm_err' id='ADDRESS_1_disp'></span>"
							+ "</div>"
							+ "</div>"
							+ "<div class='formForUserRegisterEachRowCLS'>"
							+ "<div><span style='color:red'>Locale</span></div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<div class='formForUserHasImgCLS'>"
							+ "<input disabled=disabled value='' data-label='Locale' data-regex='null' data-colname='LOCALE' data-man='M' id='LOCALE' init_value='null' type='text' />"
							+ "<img class='prop_imgClass visionMasterDetailFormddw' src='images/search_icon_color_2.png' onclick=\"visionDropdown('DDW_LOCALE_UM','','FORM-VIEW','MM_MASTER_UM_USER_DETAILS','LOCALE','')\" />"
							+ "</div>"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_LOCALE_disp'></span>"
							+ "</div>"
							+ "<div>"
							+ "<span style='color:red'>Password Flag</span>"
							+ "</div>"
							+ "<div>"
							+ "<select data-regex='null' data-label='Password Flag' data-colname='PASSWORD_FLAG' id='FFUR_PASSWORD_FLAG'>"
							+ "<option selected='selected' value='R'>R</option>"
							+ "<option value='N'>N</option>"
							+ "</select>"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_PASSWORD_FLAG_disp'></span>"
							+ "</div>"
							+ "<div>"
							+ "<span style='color:red'>Mobile No</span>"
							+ "</div>"
							+ "<div class='formForUserHasErrorMsgCLS'>"
							+ "<input maxlength='10' value='' data-label='Mobile No' data-regex='null' data-colname='MOBILE' data-man='M' id='MOBILE' init_value='null' type='text' />"
							+ "<span class='visionMasterDetailForm_err' id='FFUR_MOBILE_disp'></span>"
							+ "</div>"
							+ "</div>");
					
					resultObj.put("formData",formData);
				} 
				
				else {
					resultObj.put("status",false);
					formData.append("User Registration is not available for this SaaS Model, Please Contact Administrator.");
					resultObj.put("formData",formData);
				}
			}
			
        }
        catch (Exception ex) {
			ex.printStackTrace();
		}
    return resultObj;
    }
    
    
    
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JSONObject createUser(HttpServletRequest request, JSONObject gridData) {
        String message = "";
        String columnName = "";
        SPersDetail sPersDetail = new SPersDetail();
        SAuthorisation sAuthorisation = new SAuthorisation();
        SPersonnel sPersonnel = new SPersonnel();
        SPersOrgnLang sPersOrgnLang = new SPersOrgnLang();
        SPersProfile sPersProfile = new SPersProfile();
        //SPersDetail spersDetail=new SPersDetail();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        HttpSession httpSession = request.getSession(false);
        String ssOrgnId = (String) httpSession.getAttribute("ssOrgId");
        String ssLocale = (String) httpSession.getAttribute("ssLocale");
        String ssLanguageID = (String) httpSession.getAttribute("ssLanguageID");
        String ssUsername = (String) httpSession.getAttribute("ssUsername");
        String ssRegion = (String) httpSession.getAttribute("ssRegion");
        String ssUserlocat = (String) httpSession.getAttribute("ssUserlocat");
        String ssuserRole = (String) httpSession.getAttribute("ssRole");
        String ssInstance = (String) httpSession.getAttribute("ssInstance");
        String moduleCode = (String) request.getSession(false).getAttribute("ModuleCode");
        JSONArray jsColVals = new JSONArray();
        JSONObject jsColObj = new JSONObject();
        String returnMessage = "";
        Calendar cal = Calendar.getInstance();
        Date expiryDate = new Date();
        JSONObject jsResponse = new JSONObject();
        String languageId = "", locale = "";

        try {
//            int UserCount = getUserExistCount(request, (String) gridData.get("USER_NAME"));
            int UserCount = getUserExistCount(request,  gridData);
            if (UserCount <= 0) {
                boolean status = checkingNoOfUserCount(request, (String) gridData.get("ROLE_ID"));
                if (status) {
                    languageId = String.valueOf(gridData.get("LANGUAGE_ID"));
                    if (languageId.contains("B_LANGUAGE")) {
                        languageId = "";
                    }
                    Iterator gridDataItr = gridData.keySet().iterator();
                    if (!(languageId !=null && !"".equalsIgnoreCase(languageId) && !"null".equalsIgnoreCase(languageId))) {
                        locale = String.valueOf(gridData.get("LOCALE"));
                        String query = "select LANGUAGE_ID FROM B_LANGUAGE WHERE COUNTRY_CODE=:COUNTRY_CODE AND LANGUAGE_CODE=:LANGUAGE_CODE";
                        Map dataMap = new HashMap<>();
                        dataMap.put("COUNTRY_CODE", locale.split("_")[1].toUpperCase());//SQL Injection change
                        dataMap.put("LANGUAGE_CODE", locale.split("_")[0]);//SQL Injection change
                        List listLangCode = access.sqlqueryWithParams(query, dataMap);
                        if (!listLangCode.isEmpty()) {
                            languageId = (String) listLangCode.get(0);
                            gridData.put("LANGUAGE_ID", languageId);
                        }

                    }
                    
                    sPersDetail.setUserName(gridData.get("USER_NAME") == null ? "" : String.valueOf(gridData.get("USER_NAME")).toUpperCase());
                    sPersDetail.setFirstName(gridData.get("FIRST_NAME") == null ? "" : String.valueOf(gridData.get("FIRST_NAME")));
                    sPersDetail.setMiddleName(gridData.get("MIDDLE_NAME") == null ? "" : String.valueOf(gridData.get("MIDDLE_NAME")));
                    sPersDetail.setLastName(gridData.get("LAST_NAME") == null ? "" : String.valueOf(gridData.get("LAST_NAME")));
                    sPersDetail.setAddress1(gridData.get("ADDRESS_1") == null ? "" : String.valueOf(gridData.get("ADDRESS_1")));
                    sPersDetail.setAddress2(gridData.get("ADDRESS_2") == null ? "" : String.valueOf(gridData.get("ADDRESS_2")));
                    sPersDetail.setExpSummary(gridData.get("EXP_SUMMARY") == null ? "" : String.valueOf(gridData.get("EXP_SUMMARY")));
                    sPersDetail.setPurposeReg(gridData.get("PURPOSE_REG") == null ? "" : String.valueOf(gridData.get("PURPOSE_REG")));
                    sPersDetail.setPersId(AuditIdGenerator.genRandom32Hex());
                    
                    jsColVals = new JSONArray();
                    jsColObj = new JSONObject();
                    jsColObj.put("colName", "COUNTRY_CODE");
                    jsColObj.put("colVal", gridData.get("COUNTRY"));
                    jsColVals.add(jsColObj);
                    sPersDetail.setBCountry((BCountry)getCountry((String)gridData.get("COUNTRY")));
                    String dateString = String.valueOf(gridData.get("DOB"));
                    String[] splitDate;
                    if (dateString.length() == 10) {
                        splitDate = dateString.split("-");
                        if (splitDate[0].length() == 4) {
                            cal.set(Integer.parseInt(splitDate[0]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[2]));

                        } else {

                            cal.set(Integer.parseInt(splitDate[2]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[0]));
                        }
                    } else {
                        splitDate = dateString.substring(0, 10).split("-");
                        if (splitDate[0].length() == 4) {
                            cal.set(Integer.parseInt(splitDate[0]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[2]));

                        } else {

                            cal.set(Integer.parseInt(splitDate[2]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[0]));
                        }
                    }
                    
                    LocalDate currentDate = LocalDate.now();
                    LocalDate oneYearLater = currentDate.plusYears(1);
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    String oneYearLaterDate = oneYearLater.format(formatter);
                   
                    sPersDetail.setDob(cal.getTime());
                    sPersDetail.setEmail(gridData.get("EMAIL") == null ? "" : String.valueOf(gridData.get("EMAIL")));
                    sPersDetail.setMobile(gridData.get("MOBILE") == null ? "" : String.valueOf(gridData.get("MOBILE")));
                    sPersDetail.setTelephone(gridData.get("TELEPHONE") == null ? "" : String.valueOf(gridData.get("TELEPHONE")));
                    //  sPersDetail.setUserName(gridData.get("USER_NAME") == null ? "" : String.valueOf(gridData.get("USER_NAME")));
                    sPersDetail.setCreateBy(ssUsername);
                    sPersDetail.setAuditId(AuditIdGenerator.getAuditId("S_PERS_DETAIL"));
                    sPersDetail.setEditBy(ssUsername);
                    sPersDetail.setCreateDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    sPersDetail.setEditDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    dateString = String.valueOf(gridData.get("EXPIRY_DATE"));
                    dateString = (dateString == "null") ? oneYearLaterDate : dateString; 
//            splitDate = dateString.substring(0, 10).split("-");
//            cal.set(Integer.parseInt(splitDate[2]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[0]));

                    if (dateString.length() == 10) {
                        splitDate = dateString.split("-");
                        if (splitDate[0].length() == 4) {
                            cal.set(Integer.parseInt(splitDate[0]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[2]));

                        } else {

                            cal.set(Integer.parseInt(splitDate[2]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[0]));
                        }
                    } else {
                        splitDate = dateString.substring(0, 10).split("-");
                        if (splitDate[0].length() == 4) {
                            cal.set(Integer.parseInt(splitDate[0]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[2]));

                        } else {
                            cal.set(Integer.parseInt(splitDate[2]), Integer.parseInt(splitDate[1]) - 1, Integer.parseInt(splitDate[0]));
                        }
                    }
                    expiryDate = cal.getTime();
                    sPersDetail.setExpiryDate(cal.getTime());
                    String device_detailsString = new PilogUtilities().getDeviceDetails(request, macCommand);
                    JSONObject deviceDetails = (JSONObject) JSONValue.parse(device_detailsString);
                    sPersDetail.setIpAddress((String) deviceDetails.get("IPAddress"));
                    sPersProfile = new SPersProfile();
                    SPersProfileId persProfileId = new SPersProfileId();
                    sPersProfile.setReportTo(gridData.get("REPORT_TO") == null ? "" : String.valueOf(gridData.get("REPORT_TO")));
                    sPersProfile.setDefaultInd("Y");
                    persProfileId.setInstance(gridData.get("INSTANCE") == null ? "100" : String.valueOf(gridData.get("INSTANCE")));
                    persProfileId.setLocale(gridData.get("LOCALE") == null ? "" : String.valueOf(gridData.get("LOCALE")));
                    //  persProfileId.setProcess(gridData.get("MODULE") == null ? "" : String.valueOf(gridData.get("MODULE")));
                    persProfileId.setPlant(gridData.get("PLANT") == null ? "10" : String.valueOf(gridData.get("PLANT")));
                    persProfileId.setRegion(String.valueOf(gridData.get("COUNTRY")) == null ? "":String.valueOf(gridData.get("COUNTRY")));
                    persProfileId.setRoleId(gridData.get("ROLE_ID") == null ? "" : String.valueOf(gridData.get("ROLE_ID")));
                    sPersProfile.setId(persProfileId);
                    sPersProfile.setCreateBy(ssUsername);
                    sPersProfile.setEditBy(ssUsername);
                    sPersProfile.setAuditId(AuditIdGenerator.getAuditId("S_PERS_PROFILE"));
                    sPersProfile.setCreateDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    sPersProfile.setEditDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    sPersProfile.setSPersDetail(sPersDetail);
                    Set<SPersProfile> persProfiles = new HashSet<>();
                    persProfiles.add(sPersProfile);
                    sPersDetail.setSPersProfiles(persProfiles);
                    PilogEncryption genEnc = new PilogEncryption();
                    sAuthorisation = new SAuthorisation();
                    String[] dobString = String.valueOf(gridData.get("DOB")).split("-");

                    SAuthorisationId authorisationId = new SAuthorisationId();
                    String dobMonth = String.valueOf(sPersDetail.getDob().getMonth() + 1);
                    if (dobMonth.length() == 1) {
                        dobMonth = "0" + dobMonth;
                    }
                    String dobDay = String.valueOf(sPersDetail.getDob().getDate());
                    if (dobDay.length() == 1) {
                        dobDay = "0" + dobDay;
                    }
                    String defaultPassword = (String) request.getSession(false).getAttribute("ssDefaultPassword");
                    if (!(defaultPassword != null && !"".equalsIgnoreCase(defaultPassword)) && !"null".equalsIgnoreCase(defaultPassword)) {
                        defaultPassword = "P@ssw0rd";
                    }
                    String passPhrase = new PilogEncryption().encrypt(defaultPassword,
                            String.valueOf(gridData.get("USER_NAME")).toUpperCase(),
                            (String) request.getSession(false).getAttribute("ssOrgId"));
//                    String.valueOf(gridData.get("USER_NAME")).toUpperCase(),
//                    dobString[2] + "-" + (dobString[1].length() == 1 ? "0" + dobString[1] : dobString[1]) + "-" + dobString[0]
//            );

                    authorisationId.setPassPhrase(passPhrase);
                    sAuthorisation.setId(authorisationId);
                    sAuthorisation.setActivationMode(gridData.get("ACTIVATION_MODE") == null ? "MOBILE" : String.valueOf(gridData.get("ACTIVATION_MODE")));
                    sAuthorisation.setOtp("123456");
                    sAuthorisation.setActivationDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    sAuthorisation.setActivatedBy("ADMIN");

                    sAuthorisation.setCreateBy(ssUsername);
                    sAuthorisation.setEditBy(ssUsername);
                    sAuthorisation.setAuditId(AuditIdGenerator.getAuditId("S_AUTHORISATION"));
                    sAuthorisation.setCreateDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    sAuthorisation.setEditDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    sAuthorisation.setSPersDetail(sPersDetail);
                    Set<SAuthorisation> authorisations = new HashSet<>();
                    authorisations.add(sAuthorisation);
                    sPersDetail.setSAuthorisations(authorisations);
                    sPersDetail = access.save(sPersDetail);
                    persProfileId.setPlant(languageId);
                    persProfileId.setPersId(sPersDetail.getPersId());
                    sPersProfile.setId(persProfileId);

                    access.save(sPersProfile);
                    authorisationId.setPersId(sPersDetail.getPersId());
                    sAuthorisation.setId(authorisationId);
                    access.save(sAuthorisation);
                    SPersonnel personnel = new SPersonnel();
                    cal = Calendar.getInstance();

                    if (gridData.get("EXPIRY_DATE") == null) {
                        cal.add(Calendar.MONTH, 6);
                        personnel.setExpiryDate(cal.getTime());
                    } else {
                        personnel.setExpiryDate(expiryDate);
                    }
                    personnel.setCreateBy(ssUsername);
                    personnel.setEditBy(ssUsername);
                    personnel.setAuditId(AuditIdGenerator.getAuditId("S_PERSONNEL"));
                    personnel.setCreateDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    personnel.setEditDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    personnel.setSPersDetail(sPersDetail);

                    personnel.setPersId(sPersDetail.getPersId());
                    if (gridData.get("PASSWORD_FLAG") == null) {
                        personnel.setPasswordFlag("A");
                    } else if (String.valueOf(gridData.get("PASSWORD_FLAG")).trim().equalsIgnoreCase("")) {
                        personnel.setPasswordFlag("A");
                    } else {
                        personnel.setPasswordFlag((String) gridData.get("PASSWORD_FLAG"));
                    }

                    personnel.setLoginAttempts(BigInteger.ZERO);
                    personnel.setStatus(String.valueOf(gridData.get("STATUS")));
                    jsColVals = new JSONArray();
                    jsColObj = new JSONObject();
                    jsColObj.put("ORGN_ID", ssOrgnId);
                    jsColObj.put("colName", "ORGN_ID");
                    jsColObj.put("colVal", ssOrgnId);
                    jsColVals.add(jsColObj);

                    personnel.setOrgnStructure((OrgnStructure) access.getPersEntityObj("ORGN_STRUCTURE", jsColVals));
                    access.save(personnel);
                    SPersOrgnLang orgnLang = new SPersOrgnLang();
                    orgnLang.setCreateBy(ssUsername);
                    orgnLang.setAuditId(AuditIdGenerator.getAuditId("S_PERS_ORGN_LANG"));
                    orgnLang.setCreateDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    orgnLang.setEditBy(ssUsername);
                    orgnLang.setEditDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                    orgnLang.setDefaultFlag(gridData.get("DEFAULT_FLAG") == null ? "Y" : String.valueOf(gridData.get("DEFAULT_FLAG")));
                    SPersOrgnLangId id = new SPersOrgnLangId();
                    id.setLanguageId(languageId);
                    id.setLocale((String) gridData.get("LOCALE"));
                    id.setPersId(sPersDetail.getPersId());
                    id.setOrgnId(ssOrgnId);
                    orgnLang.setId(id);
                    access.save(orgnLang);

                    if (gridData.containsKey("PERS_AUTH_COL2") && gridData.get("PERS_AUTH_COL2") != null
                            && !"".equalsIgnoreCase((String) gridData.get("PERS_AUTH_COL2")) && !"null".equalsIgnoreCase((String) gridData.get("PERS_AUTH_COL2"))) {

                        SPersStgAuth persAuth = new SPersStgAuth();
                        persAuth.setPersId(sPersDetail.getPersId());
                        persAuth.setPersAuthCol2((String) gridData.get("PERS_AUTH_COL2"));
                        persAuth.setPersAuthCol3((String) gridData.get("PERS_AUTH_COL3"));
                        persAuth.setCreateDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                        persAuth.setCreateBy(ssUsername);
                        persAuth.setEditDate(new PilogUtilities().getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
                        persAuth.setEditBy(ssUsername);
                        persAuth.setAuditId(AuditIdGenerator.getAuditId("S_PERS_STG_AUTH"));
                        access.save(persAuth);

                    }
                    SPersStgAuth sPersStgAuth = new SPersStgAuth();
                    sPersStgAuth.setPersId(sPersDetail.getPersId());
                    sPersStgAuth.setPersAuthCol2(gridData.get("ROLE_ID") == null ? "" : String.valueOf(gridData.get("ROLE_ID")));
                    //col 31 will store the expiry date of the user if the date is crossed redirects to changePass to update to new password
                    //col 30 will store the notification day number(example 60 days, 30 days) after which the user has to update its password
                    sPersStgAuth.setPersAuthCol31Date(new Date());
                    sPersStgAuth.setPersAuthCol30("30");
                    sPersStgAuth.setCreateBy(gridData.get("USER_NAME") == null ? "" : String.valueOf(gridData.get("USER_NAME")).toUpperCase());
                    sPersStgAuth.setCreateDate(new Date());
                    sPersStgAuth.setEditBy(gridData.get("USER_NAME") == null ? "" : String.valueOf(gridData.get("USER_NAME")).toUpperCase());
                    sPersStgAuth.setEditDate(new Date());
                    sPersStgAuth.setAuditId(AuditIdGenerator.getAuditId("S_PERS_STG_AUTH"));
                    access.save(sPersStgAuth);
                    RegistrationDTO registrationDTO = new RegistrationDTO();
                    registrationDTO.setAddress1(gridData.get("ADDRESS_1") == null ? "" : String.valueOf(gridData.get("ADDRESS_1")));
                    registrationDTO.setAddress2(gridData.get("ADDRESS_2") == null ? "" : String.valueOf(gridData.get("ADDRESS_2")));
                    registrationDTO.setConfirm_password("Pilog@123");
                    registrationDTO.setCountry(String.valueOf(gridData.get("COUNTRY")) == null ? "":String.valueOf(gridData.get("COUNTRY")));
                    registrationDTO.setDate(sPersDetail.getCreateDate().getDate()
                            + "-" + (sPersDetail.getCreateDate().getMonth() + 1)
                            + "-" + (sPersDetail.getCreateDate().getYear() + 1900));
                    registrationDTO.setDate_of_birth(sPersDetail.getDob().getDate()
                            + "-" + (sPersDetail.getDob().getMonth() + 1)
                            + "-" + (sPersDetail.getDob().getYear() + 1900));
                    registrationDTO.setEmail_id(sPersDetail.getEmail());
                    registrationDTO.setFirst_name(sPersDetail.getFirstName());
                    registrationDTO.setLast_name(sPersDetail.getLastName());
                    registrationDTO.setLocale(String.valueOf(gridData.get("LOCALE")));
                    registrationDTO.setMiddle_name(sPersDetail.getMiddleName());
                    registrationDTO.setMobile_number(sPersDetail.getMobile());
                    registrationDTO.setMonth(String.valueOf(sPersDetail.getDob().getMonth() + 1));
                    registrationDTO.setNick_name(sPersDetail.getNickName());
                    String ssOrgname = (String) request.getSession(false).getAttribute("ssOrgname");
                    registrationDTO.setOrgName(ssOrgname);
                    registrationDTO.setPassword("Pilog@123");
                    registrationDTO.setPhone_number(sPersDetail.getMobile());
                    registrationDTO.setPlant(String.valueOf(gridData.get("PLANT")));
                    registrationDTO.setRegion(String.valueOf(gridData.get("COUNTRY")) == null ? "":String.valueOf(gridData.get("COUNTRY")));
                    registrationDTO.setRole(gridData.get("ROLE_ID") == null ? "" : String.valueOf(gridData.get("ROLE_ID")));
                    registrationDTO.setUser_name(String.valueOf(gridData.get("USER_NAME")));
                    registrationDTO.setYear(String.valueOf(sPersDetail.getCreateDate().getYear() + 1900));
                    jsResponse.put("dto", registrationDTO);
                    message = "User " + gridData.get("USER_NAME") + " created successfully";
                    jsResponse.put("status", 1);
                    jsResponse.put("message", message);
                    jsResponse.put("persid", sPersDetail.getPersId());
                } else {
                    message = "Users Are Exceeded.";
                    jsResponse.put("status", 2);
                    jsResponse.put("message", message);
                    //jsResponse.put("persid", sPersDetail.getPersId());
                }
            } else {
                message = "User Already exists";
                jsResponse.put("status", 3);
                jsResponse.put("message", message);
            }

        } catch (Exception e) {
            e.printStackTrace();
            message = "Unable to create user";
            jsResponse.put("status", 0);
            jsResponse.put("message", message);
            jsResponse.put("persid", "");
        }
        return jsResponse;
    }
    
    
    @Transactional
    public Integer userExists(HttpServletRequest request) {
        String userName = request.getParameter("USER_NAME");
        String query = "SELECT USER_NAME FROM S_PERS_DETAIL WHERE"
                + " UPPER(USER_NAME)=:USER_NAME";
        HashMap<String, Object> map = new HashMap<String, Object>();
        List listUsers = new ArrayList();
        try {
            map.put("USER_NAME", userName.toUpperCase());
            listUsers = access.sqlqueryWithParams(query, map);

            if (listUsers.size() > 0) {
                System.out.println("");
            }
            System.out.println("users size::" + listUsers.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return listUsers.size();
    }
    
    
    @Transactional
    public Integer getUserExistCount(HttpServletRequest request, String userName) {
        List usersCount = new ArrayList();
        try {
            String userCountQuery = "SELECT PERS_ID FROM S_PERS_DETAIL WHERE USER_NAME=:USER_NAME";
            HashMap<String, Object> map = new HashMap<String, Object>();
            map.put("USER_NAME", userName.toUpperCase());
            usersCount = access.sqlqueryWithParams(userCountQuery, map);
            System.out.println("usersCount -->" + usersCount.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return usersCount.size();
    }
    @Transactional
    public Integer getUserExistCount(HttpServletRequest request, JSONObject gridData) {
    	List usersCount = new ArrayList();
    	try {
    		String userName = (String) gridData.get("USER_NAME");
    		String email = (String) gridData.get("EMAIL");
    		String userCountQuery = "SELECT PERS_ID FROM S_PERS_DETAIL WHERE USER_NAME=:USER_NAME AND UPPER(EMAIL) = UPPER('"+email+"')";
    		HashMap<String, Object> map = new HashMap<String, Object>();
    		map.put("USER_NAME", userName.toUpperCase());
    		usersCount = access.sqlqueryWithParams(userCountQuery, map);
    		System.out.println("usersCount -->" + usersCount.size());
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	return usersCount.size();
    }
    
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean checkingNoOfUserCount(HttpServletRequest request, String cellData) {
        boolean isProcess = false;
        HttpSession httpSession = request.getSession(false);
        String ssOrgnId = (String) httpSession.getAttribute("ssOrgId");
        String roleId = cellData;

        try {
            String roleUsersQuery = "SELECT NO_OF_USERS FROM C_ROLE_USERS WHERE ORGN_ID =:ORGN_ID AND ROLE_ID =:ROLE_ID";

            Map<String, Object> roleUsersMap = new HashMap<>();

            roleUsersMap.put("ORGN_ID", ssOrgnId);
            roleUsersMap.put("ROLE_ID", roleId);
            List roleUsersList = access.sqlqueryWithParams(roleUsersQuery, roleUsersMap);
            BigDecimal next = new BigDecimal(0);
            for (Iterator iterator = roleUsersList.iterator(); iterator.hasNext();) {
                next = (BigDecimal) iterator.next();
                System.out.println("The roleUsersList value is:::" + next);
            }
            int roleUsersCount = next.intValue();

            String noOfUserQuery = "SELECT DISTINCT USER_NAME FROM RPRT_PERS_DETAILS WHERE ROLE_ID =:ROLE_ID AND ORGN_ID =:ORGN_ID AND STATUS ='ACTIVE'";
            Map<String, Object> persDetailMap = new HashMap<>();
            persDetailMap.put("ORGN_ID", ssOrgnId);
            persDetailMap.put("ROLE_ID", roleId);
            List noOfUserList = access.sqlqueryWithParams(noOfUserQuery, persDetailMap);
            if (noOfUserList != null && !noOfUserList.isEmpty()) {
                if (noOfUserList.size() < roleUsersCount) {
                    isProcess = true;

                } else {
                    isProcess = false;

                }
            } else {
                isProcess = true;
            }

        } // }
        catch (Exception e) {
            e.printStackTrace();
        }
        //String str = String.valueOf(isProcess);
        return isProcess;

    }
    
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Date getExpiryDate(String orgnId) {
        Date expiryDate = null;
        try {
            String roleUsersQuery = 
                "SELECT EXPIRY_DATE FROM ORGN_SUBSCRIPTION_DETAILS WHERE ORGN_ID = :ORGN_ID";
                                       
            Map<String, Object> orgnMap = new HashMap<>();
            orgnMap.put("ORGN_ID", orgnId);

            List orgnList = access.sqlqueryWithParams(roleUsersQuery, orgnMap);
            if (orgnList != null && !orgnList.isEmpty()) {
                Object result = orgnList.get(0);
                if (result instanceof Date) {
                    expiryDate = (Date) result;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return expiryDate;
    }


}
