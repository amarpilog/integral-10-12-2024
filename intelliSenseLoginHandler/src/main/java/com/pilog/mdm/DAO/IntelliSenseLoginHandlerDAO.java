/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.DAO;

import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogEncryption;
import com.pilog.mdm.utilities.PilogUtilities;
import com.pilog.mdm.DTO.LoginHandler;
import com.pilog.mdm.access.DataAccess;
import com.pilog.mdm.pojo.BApplProperties;
import com.pilog.mdm.pojo.BLanguage;
import com.pilog.mdm.pojo.BSessionsId;
import com.pilog.mdm.pojo.CRoleUsers;
import com.pilog.mdm.pojo.SAuthorisation;
import com.pilog.mdm.pojo.SClientLicence;
import com.pilog.mdm.pojo.SPersAudit;
import com.pilog.mdm.pojo.SPersConcurr;
import com.pilog.mdm.pojo.SPersDetail;
import com.pilog.mdm.pojo.SPersOrgnLang;
import com.pilog.mdm.pojo.SPersProfile;
import com.pilog.mdm.pojo.SPersnolisation;
import com.pilog.mdm.pojo.SPersnolisationId;
import com.pilog.mdm.pojo.SPersonnel;
import com.pilog.mdm.service.HttpSessionListenerPublisher;

import java.sql.Blob;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.sql.rowset.serial.SerialBlob;

import oracle.sql.RAW;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
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
public class IntelliSenseLoginHandlerDAO {

	@Autowired
	private DataAccess access;

	String success = "success";
	String userPasswordInvalid = "userPasswordInvalid";
	String userLocked = "userLocked";
	String userPasswordInvalid2 = "userPasswordInvalid2";
	String userInactive = "userInactive";
	String expiryInvalid = "expiryInvalid";
	String userPasswordExpiry = "userPasswordExpiry";
	String userPasswordExpiry2 = "userPasswordExpiry2";
	String applicationUnavailable = "applicationUnavailable";
	String error = "error";
	String passwordReset = "passwordReset";
	String passwordExpiryReset = "passwordExpiryReset";
	String passwordFirstLogin = "passwordFirstLogin";
	String concurrent = "concurrent";
	String alreadyLoggedIn = "alreadyLoggedIn";
	String languageFailed = "languageFailed";
	String userNotExist = "userNotExist";
	String ldapUserNotExist = "ldapUserNotExist";
	String samlUserNotExist = "samlUserNotExist";
	String userNotAuth = "userNotAuth";
	@Value("${Application.enable.SSO}")
	private String enableSSO;
	@Value("${hibernate.search.default.indexBase}")

	private String macCommand;

	private PilogUtilities cloudUtills = new PilogUtilities();

	@Autowired
	private IntelliSenseLoginHandlerOperationsDAO loginHandlerOperationsDAO;

//    public JSONObject loginHandler(LoginHandler loginHandler, HttpServletRequest request) {
//        String result = "";
//        JSONObject sessionObj = new JSONObject();
//        JSONObject resultObj = new JSONObject();
//        try {
//
//            SPersDetail persDetail = getPersDetailsByUsername(loginHandler.getRsUsername());
//
//            if (persDetail != null) {
//
//                String userAuth = checkUserAuth(persDetail, request);
//                if (userAuth != null && !"".equalsIgnoreCase(userAuth)) {
//                    result = userAuth;
//                } else {
//                    SPersonnel personnel = getPersonnelByPersId(persDetail.getPersId());
//                    if (personnel != null) {
////                        String langCode = checkLanguageByUser(loginHandler, request);
////                        if (langCode != null && "languageSuccess".equalsIgnoreCase(langCode)) {
//                        if (true) {
//                            if (true) {
////                            if (checkingNoOfUserCount(persDetail, personnel)) {
//                                // need to write otp 
//                                if (true) {
////                                if (isOTPSame(persDetail, loginHandler)) {
//                                    String licenceKey = "success";
////                                    String licenceKey = clientLicenceMACExpiry(personnel.getOrgnStructure().getOrgnId());
//                                    System.out.println("licenceKey:::::" + licenceKey);
//                                    if (licenceKey != null && !"".equalsIgnoreCase(licenceKey) && "success".equalsIgnoreCase(licenceKey)) {
//                                        //LOGIN_FAILURE_ATTEMPTS
//                                        int loginFailureAttempts = 3;
//                                        BApplProperties loginFailureAttApplProperties = (BApplProperties) getApplProperties("LOGIN_FAILURE_ATTEMPTS");
//                                        if (loginFailureAttApplProperties != null) {
//                                            String loginAttemptsStr = loginFailureAttApplProperties.getId().getProcessValue();
//                                            if (loginFailureAttApplProperties.getId().getProcessValue() != null
//                                                    && !"".equalsIgnoreCase(loginFailureAttApplProperties.getId().getProcessValue())
//                                                    && !"0".equalsIgnoreCase(loginFailureAttApplProperties.getId().getProcessValue())
//                                                    && cloudUtills.isNumeric(loginFailureAttApplProperties.getId().getProcessValue())) {
//                                                loginFailureAttempts = Integer.parseInt(loginFailureAttApplProperties.getId().getProcessValue());
//                                            }
//                                        }
//                                        if (personnel.getLoginAttempts().intValue() <= loginFailureAttempts) {
//                                            //logout other users 
//                                            List sPersAuditList = (List) getLoginUserAudit(persDetail.getPersId());
//                                            if (sPersAuditList != null && !sPersAuditList.isEmpty()) {
//
//                                                if (sPersAuditList.size() >= 1) {
//                                                    if (loginHandler != null && "Y".equalsIgnoreCase(loginHandler.getSsAutoStart())) {
//                                                        // if already sessions are available need to kill that sessions by persId
//                                                        try {
////                                                             loginHandlerOperationsDAO.updateLogOutByAllPersId(persDetail.getPersId(), request);
//                                                        } catch (Exception e) {
//                                                        }
//                                                    } else {
//                                                        result = alreadyLoggedIn;
//                                                    }
//
//                                                }
//
//                                            }
//                                            JSONObject loginObj = login(request, loginHandler, persDetail, personnel, result);
//                                            result = (String) loginObj.get("resultCode");
//                                            sessionObj = (JSONObject) loginObj.get("sessionObj");
//                                        } else {
//                                            result = userLocked;
//                                        }
//                                    } else {
//                                        result = licenceKey;
////                     result = "LICENCE";
//                                    }
//                                } else {
//
//                                    result = "OTP";
//                                }
//                            } else {
//                                result = "USERS_EXCEEDED";
//                            }
//                        } else {
//                            result = languageFailed;
//                        }
//
//                    } else {
//                        //System.err.println("**** User Not Exist in S_PERSONNEL****");
//                        result = userNotExist;
//                    }
//                }
//            } else {
//                //System.err.println("**** User Not Exist in S_PERS_DETAIL****");
//                result = userNotExist;
//            }
//            resultObj.put("resultCode", result);
//            resultObj.put("sessionObj", sessionObj);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return resultObj;
//    }

	public JSONObject loginHandler(LoginHandler loginHandler, HttpServletRequest request) {
		String result = "";
		JSONObject sessionObj = new JSONObject();
		JSONObject resultObj = new JSONObject();
		try {

			SPersDetail persDetail = getPersDetailsByUsername(loginHandler.getRsUsername());

			if (persDetail != null) {

				String userAuth = checkUserAuth(persDetail, request);
				if (userAuth != null && !"".equalsIgnoreCase(userAuth)) {
					result = userAuth;
				} else {
					SPersonnel personnel = getPersonnelByPersId(persDetail.getPersId());
					String imageurl = getPersnolisationImageUrl(persDetail.getPersId()); // som
					request.getSession(false).setAttribute("imageurl", imageurl);
					resultObj.put("profile_imgStr", imageurl);
					if (personnel != null) {
						// String langCode = checkLanguageByUser(loginHandler, request); //ssss
						// if (langCode != null && "languageSuccess".equalsIgnoreCase(langCode)) {
						// //ssss
//                        if (true) {  //ssss
//                            if (true) {  //ssss
						if (checkingNoOfUserCount(persDetail, personnel)) { // som
							// need to write otp
							if (true) {
//                                if (isOTPSame(persDetail, loginHandler)) {
//                                    String licenceKey = "success";
								String licenceKey = clientLicenceMACExpiry(personnel.getOrgnStructure().getOrgnId());
								System.out.println("licenceKey:::::" + licenceKey);
								if (licenceKey != null && !"".equalsIgnoreCase(licenceKey)
										&& "success".equalsIgnoreCase(licenceKey)) {
									// LOGIN_FAILURE_ATTEMPTS
									int loginFailureAttempts = 3;
									BApplProperties loginFailureAttApplProperties = (BApplProperties) getApplProperties(
											"LOGIN_FAILURE_ATTEMPTS");
									if (loginFailureAttApplProperties != null) {
										String loginAttemptsStr = loginFailureAttApplProperties.getId()
												.getProcessValue();
										if (loginFailureAttApplProperties.getId().getProcessValue() != null
												&& !"".equalsIgnoreCase(
														loginFailureAttApplProperties.getId().getProcessValue())
												&& !"0".equalsIgnoreCase(
														loginFailureAttApplProperties.getId().getProcessValue())
												&& cloudUtills.isNumeric(
														loginFailureAttApplProperties.getId().getProcessValue())) {
											loginFailureAttempts = Integer
													.parseInt(loginFailureAttApplProperties.getId().getProcessValue());
										}
									}
									if (personnel.getLoginAttempts().intValue() <= loginFailureAttempts) {
										// logout other users
										List sPersAuditList = (List) getLoginUserAudit(persDetail.getPersId());
										if (sPersAuditList != null && !sPersAuditList.isEmpty()) {

											if (sPersAuditList.size() >= 1) {
												if (loginHandler != null
														&& "Y".equalsIgnoreCase(loginHandler.getSsAutoStart())
//                                                    		|| (loginHandler.getApiId() != null 
//                                                            && !"".equalsIgnoreCase(loginHandler.getApiId())
//                                                            && !"null".equalsIgnoreCase(loginHandler.getApiId()))
												) {
													// if already sessions are available need to kill that sessions by
													// persId
													try {
														loginHandlerOperationsDAO.updateLogOutByAllPersId(
																persDetail.getPersId(), request);
													} catch (Exception e) {
													}
												} else {
													result = alreadyLoggedIn;
												}

											}

										}
										JSONObject loginObj = login(request, loginHandler, persDetail, personnel,
												result);
										result = (String) loginObj.get("resultCode");
										sessionObj = (JSONObject) loginObj.get("sessionObj");
										sessionObj.put("orgnSubscriptionDefaultUser", persDetail.getUserAuth());
									} else {
										result = userLocked;
									}
								} else {
									result = licenceKey;
//                     result = "LICENCE";
								}
							} else {

								result = "OTP";
							}
						} else { // ssss
							result = "USERS_EXCEEDED";
						} // ssss
//                        } else {  //ssss
//                            result = languageFailed;
//                        } //ssss

					} else {
						// System.err.println("**** User Not Exist in S_PERSONNEL****");
						result = userNotExist;
					}
				}
			} else {
				// System.err.println("**** User Not Exist in S_PERS_DETAIL****");
				result = userNotExist;
			}
			resultObj.put("resultCode", result);
			resultObj.put("sessionObj", sessionObj);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	// getting BApplProperties based on Key name
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public Object getApplProperties(String keyName) {
		Object resultObject = null;
		try {
			String query = " from BApplProperties WHERE id.keyname =:keyname ";
			Map<String, Object> applPropMap = new HashMap<>();
			applPropMap.put("keyname", keyName);
			List applPropList = access.queryWithParams(query, applPropMap);
			if (applPropList != null && !applPropList.isEmpty()) {
				resultObject = applPropList.get(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public Object getApplProperties(List keyNameList) {
		Object resultObject = null;
		try {
			String query = " from BApplProperties WHERE id.keyname in :keyname ";
			Map<String, Object> applPropMap = new HashMap<>();
			applPropMap.put("keyname", keyNameList);
			List applPropList = access.queryWithParams(query, applPropMap);
			if (applPropList != null && !applPropList.isEmpty()) {
				resultObject = applPropList;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObject;
	}

	// Getting SPersDetail Object based on Username
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

	// Getting Login Users Count
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public int getLoginUserCount() {
		int persCount = 0;
		try {
			String query = " select count(*) from SPersAudit where flag='N'  ";
			List countList = access.queryWithParams(query, new HashMap<String, Object>());
			if (countList != null && !countList.isEmpty()) {
				persCount = ((Long) countList.get(0)).intValue();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return persCount;
	}

	// Getting MaxActiveSessions based on OrgnId
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public int getMaxActiveSessions(String orgnId) {
		int maxActiveSessions = -1;
		try {
			String query = " from SPersConcurr where orgnId =:orgnId";
			Map<String, Object> concurrMap = new HashMap<>();
			concurrMap.put("orgnId", orgnId);
			List concurrList = access.queryWithParams(query, concurrMap);
			if (concurrList != null && !concurrList.isEmpty()) {
				SPersConcurr persConcurr = (SPersConcurr) concurrList.get(0);
				if (persConcurr != null) {
					maxActiveSessions = persConcurr.getUsers().intValue();
				} else {
					// System.err.println("**** MaxActiveSessions not configured ****");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return maxActiveSessions;
	}

	// Getting SPersonnel Object Based On PersId
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public SPersonnel getPersonnelByPersId(String persId) {
		SPersonnel personnel = null;
		try {
			String query = " from SPersonnel where persId =:persId";
			Map<String, Object> personnelMap = new HashMap<>();
			personnelMap.put("persId", persId);
			List personnelList = access.queryWithParams(query, personnelMap);
			if (personnelList != null && !personnelList.isEmpty()) {
				personnel = (SPersonnel) personnelList.get(0);
			} else {
				// System.err.println("*****User Not Exist In S_PERSONNEL Table****");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return personnel;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public Object getLoginUserAudit(String persId) {
		Object object = null;
		try {
			String query = " FROM SPersAudit WHERE flag='N' and id.persId =:persId order by loginDate desc ";
			Map<String, Object> auditMap = new HashMap<>();
			auditMap.put("persId", persId);
			List auditList = access.queryWithParams(query, auditMap);
			object = auditList;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return object;
	}

	@Transactional
	public String checkUserAuth(SPersDetail persDetail, HttpServletRequest request) {
		String userAuthResult = "";
		try {
			SPersonnel personnel = getPersonnelByPersId(persDetail.getPersId());
			String userAuthInd = getOrgnPropertyvalue("USER_AUTH", "LOGIN", personnel.getOrgnStructure().getOrgnId());
			if (userAuthInd != null && "Y".equalsIgnoreCase(userAuthInd)) {
				String url = request.getRequestURL().toString();
				// String contextUrl = request.get;
				url = url.replaceAll("/loginhandler", "");
				System.out.println("Login URL:::" + url);
				System.out.println("User Auth URL:::" + persDetail.getUserAuth());
				if (persDetail.getUserAuth() != null && !"".equalsIgnoreCase(persDetail.getUserAuth())) {
					if (!persDetail.getUserAuth().toUpperCase().contains(url.toUpperCase())) {
						userAuthResult = userNotAuth;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return userAuthResult;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public String checkLanguageByUser(LoginHandler loginHandler, HttpServletRequest request) {
		String result = "";
		try {
			SPersDetail detail = getPersDetailsByUsername(loginHandler.getRsUsername().toUpperCase());
			if (detail != null) {
				SPersonnel personnel = (SPersonnel) access.loadById(SPersonnel.class, detail.getPersId());
				JSONObject langObj = featchLanguage(detail.getPersId(), personnel.getOrgnStructure().getOrgnId(),
						loginHandler.getLanguage());
				if (langObj != null && !langObj.isEmpty()) {
					List langList = (List) langObj.get("langList");
					if (langList != null && !langList.isEmpty()) {
						result = "languageSuccess";
					} else {
						result = "languageFailed";
					}
				} else {
					result = "languageFailed";
				}
			} else {
				result = "languageFailed";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Transactional
	public boolean checkingNoOfUserCount(SPersDetail persDetail, SPersonnel sPersonnel) {
		boolean isProcess = false;
		try {
			if (persDetail != null && sPersonnel != null) {

				String roleUsersQuery = " from CRoleUsers where id.orgnId =:orgnId "
						+ "and id.roleId in (select id.roleId from SPersProfile where id.persId =:persId)";
				Map<String, Object> roleUsersMap = new HashMap<>();
				roleUsersMap.put("orgnId", sPersonnel.getOrgnStructure().getOrgnId());
				roleUsersMap.put("persId", persDetail.getPersId());
				List<CRoleUsers> roleUsersList = access.queryWithParams(roleUsersQuery, roleUsersMap);
				if (roleUsersList != null && !roleUsersList.isEmpty()) {
					String noOfUserQuery = "SELECT DISTINCT USER_NAME FROM RPRT_PERS_DETAILS WHERE ORGN_ID =:ORGN_ID AND ROLE_ID =:ROLE_ID AND STATUS ='ACTIVE'";
					for (int i = 0; i < roleUsersList.size(); i++) {
						Integer roleUsersCount = 0;
						CRoleUsers cRoleUsers = roleUsersList.get(i);

						if (cRoleUsers != null) {
							roleUsersCount = cRoleUsers.getNoOfUsers().intValue();
							Map<String, Object> persDetailMap = new HashMap<>();
							persDetailMap.put("ORGN_ID", cRoleUsers.getId().getOrgnId());
							persDetailMap.put("ROLE_ID", cRoleUsers.getId().getRoleId());
							List noOfUserList = access.sqlqueryWithParams(noOfUserQuery, persDetailMap);
							if (noOfUserList != null && !noOfUserList.isEmpty()) {
								if (noOfUserList.size() <= roleUsersCount) {
									isProcess = true;
								} else {
									isProcess = false;
									break;
								}
							}
						}
					}

				} else {
					System.err.println("** Not Configured in C_ROLE_USERS Table******");
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isProcess;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public String clientLicenceMACExpiry(String orgnId) {
		String result = "success";
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		try {
			String domain = "LICENCE";
			String licenceKey = "468788271FE5050EE053460AA9C06D4F";
//            if (licenceInd != null && !"".equalsIgnoreCase(licenceInd) && "${468788271FE5050EE053460AA9C06D4F}".equalsIgnoreCase(licenceInd)) {
			BApplProperties licenceDBKey = (BApplProperties) getApplProperties(licenceKey);
			String licenceDBKeyStr = "";
			if (licenceDBKey != null && licenceDBKey.getId().getProcessValue() != null) {
				licenceDBKeyStr = licenceDBKey.getId().getProcessValue();
			}
			if (!"N".equalsIgnoreCase(PilogEncryption.decryptAES256(licenceDBKeyStr))) {
				// System.err.println(":::://System.err.println(orgnId);:::::"+orgnId);
//            System.err.println(":::://System.err.println(licenceInd);:::::"+licenceInd);
				String query = " from SClientLicence where id.clientId =:clientId";
				Map<String, Object> licenceMap = new HashMap<>();
				licenceMap.put("clientId", orgnId);
				List licenceList = access.queryWithParams(query, licenceMap);
				if (licenceList != null && !licenceList.isEmpty()) {
					SClientLicence clientLicence = (SClientLicence) licenceList.get(0);
					if (clientLicence != null) {
						// write code about Client Licence
						if (clientLicence.getToDate() != null) {
//                     
							Date currenDate = new Date();
							Date expiryDate = clientLicence.getToDate();
							Date fromDate = clientLicence.getFromDate();
							expiryDate.setDate(expiryDate.getDate() + 1);
							if (!currenDate.after(expiryDate) && !currenDate.before(fromDate)) {

								String serverMacAddress = "";
								Map serverMacAddressMap = cloudUtills.getMacAddress(macCommand);
								serverMacAddress = (String) serverMacAddressMap.get("MACAddress");
//                                    System.out.println("serverMacAddress:::" + serverMacAddress);
								if (serverMacAddress != null && !"".equalsIgnoreCase(serverMacAddress)) {
									String orgnPropQuery = " SELECT COUNT(*) FROM C_ORGN_PROPERTIES WHERE  ORGN_ID =:ORGN_ID"
											+ " AND DOMAIN =:DOMAIN AND PROPERTY_NAME =:PROPERTY_NAME AND PROPERTY_VALUE LIKE '%"
											+ serverMacAddress + "%'";
									Map<String, Object> macMap = new HashMap<>();
									macMap.put("ORGN_ID", orgnId);
									macMap.put("DOMAIN", domain);
									macMap.put("PROPERTY_NAME", licenceKey);
//                                        macMap.put("PROPERTY_VALUE", serverMacAddress);
									int macCount = 0;
									List macPropList = access.sqlqueryWithParams(orgnPropQuery, macMap);
									if (macPropList != null && !macPropList.isEmpty()) {
										macCount = cloudUtills.convertIntoInteger(macPropList.get(0));
										if (macCount != 0) {

											System.err.println("clientLicence key  matched to MAC Address");
											String encryptedStr = getGeneratedLicenceKey(orgnId);
											if (encryptedStr != null && !"".equalsIgnoreCase(encryptedStr)
													&& !"null".equalsIgnoreCase(encryptedStr)) {
												String descryptedStr = PilogEncryption.decryptAES256(encryptedStr);
												if (descryptedStr != null && !"".equalsIgnoreCase(descryptedStr)
														&& !"null".equalsIgnoreCase(descryptedStr)
														&& descryptedStr.contains(serverMacAddress)) {
													String[] expiryLicenceArray = descryptedStr.split(orgnId);
													if (expiryLicenceArray != null && expiryLicenceArray.length != 0) {
														String expiryDateStr = expiryLicenceArray[1];
//                                                            System.out.println("expiryDateStr:::"+expiryDateStr);
														if (expiryDateStr != null && !"".equalsIgnoreCase(expiryDateStr)
																&& !"null".equalsIgnoreCase(expiryDateStr)) {
															Date licenceExpiryDate = sdf.parse(expiryDateStr);
//                                                                 System.out.println("encExpiryDate:::"+licenceExpiryDate);
															if (licenceExpiryDate != null) {
																licenceExpiryDate
																		.setDate(licenceExpiryDate.getDate() + 1);
//                                                                    System.out.println("encExpiryDate::::"+licenceExpiryDate);
																if (currenDate.after(licenceExpiryDate)) {
																	System.err.println(
																			"**** Client Licence Expiry Date Exceded ****");
																	result = "LICENCE";
																} else {
																	System.err.println(
																			"Client Licence Expiry Date Not Exceded ****");
																}
															}
														}
													}
												} else {
													System.err.println(
															"MAC Address Not Configurable Configuration Table/Not Matched");
													result = "LICENCE_KEY";
												}
											}
										} else {
											System.err.println("clientLicence key Not matched to MAC Address");
											result = "LICENCE_KEY";
										}
									} else {
										System.err.println(
												"MAC Address Not Configurable Configuration Table/Not Matched");
										result = "LICENCE_KEY";
									}
								} else {
									result = "LICENCE_KEY";
									System.err.println("** Failed to Get MAC Address *****");
								}

							} else {

								System.err.println("**** Client Licence Expired ****");
								result = "LICENCE";
							}
						} else {
							// System.err.println("***** Client To Date Not Configured *****************");
							result = "LICENCE";
							System.err.println("**** Client Licence To Date Not Configured ****");
						}
					} else {
						result = "LICENCE";
						System.err.println("**** Client Licence Info Not Configured ****");
					}
				} else {
					result = "LICENCE";
					System.err.println("**** Client Licence Info Not Configured ****");
				}
			}
//            }

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public JSONObject login(HttpServletRequest request, LoginHandler loginHandler, SPersDetail persDetail,
			SPersonnel personnel, String concurrentStatus) {
		String resultCode = "";
		long dag = 86400000L;
		JSONObject loginObj = new JSONObject();
		JSONObject sessionObj = new JSONObject();
		int warningTime = 7;
		String role = (String) request.getSession(false).getAttribute("ssRole");
		int noOfErrorLogins = 999999;
		try {
			if (persDetail != null) {
				if (personnel != null) {
					role = "MM_MANAGER";
					if (loginHandler.getRsUsername() != null && !"".equalsIgnoreCase(loginHandler.getRsUsername())
							&& !"null".equalsIgnoreCase(loginHandler.getRsUsername())
							&& loginHandler.getRsUsername().contains("REQ")) {
						role = "MM_REQUESTOR";
					} else if (loginHandler.getRsUsername() != null
							&& !"".equalsIgnoreCase(loginHandler.getRsUsername())
							&& !"null".equalsIgnoreCase(loginHandler.getRsUsername())
							&& loginHandler.getRsUsername().contains("APP")) {
						role = "MM_APPROVER";
					} else if (loginHandler.getRsUsername() != null
							&& !"".equalsIgnoreCase(loginHandler.getRsUsername())
							&& !"null".equalsIgnoreCase(loginHandler.getRsUsername())
							&& loginHandler.getRsUsername().contains("MGR")) {
						role = "MM_MANAGER";
					} else if (loginHandler.getRsUsername() != null
							&& !"".equalsIgnoreCase(loginHandler.getRsUsername())
							&& !"null".equalsIgnoreCase(loginHandler.getRsUsername())
							&& loginHandler.getRsUsername().contains("STD")) {
						role = "MM_STEWARD";
					}

					SAuthorisation authorisation = (SAuthorisation) getAuthorisation(persDetail.getPersId());
					if (authorisation != null) {
						String encPassword = authorisation.getId().getPassPhrase();
						String password = loginHandler.getRsPassword();
						resultCode = checkActiveExpiry(persDetail, personnel, dag, warningTime);
						if (resultCode.equalsIgnoreCase(success)) {
							if (new PilogEncryption()
									.encrypt(loginHandler.getRsPassword(), loginHandler.getRsUsername(), role)
									.equals(encPassword)) {
								// password match
								resultCode = success;
							} else if (new PilogEncryption().encrypt(loginHandler.getRsPassword().toUpperCase(),
									loginHandler.getRsUsername(), personnel.getOrgnStructure().getOrgnId())
									.equals(encPassword)) {
								// password match
								resultCode = success;
							} else if (new PilogEncryption().encrypt(loginHandler.getRsPassword(),
									loginHandler.getRsUsername(), personnel.getOrgnStructure().getOrgnId())
									.equals(encPassword)) {
								// 28222 this is for old users whos values are encrypted by orgnID and not roles
								resultCode = success;
							} else {

								resultCode = userPasswordInvalid2;
							}

						}

					} else {
						// System.err.println("*** SAuthorisation Empty****");
						resultCode = userPasswordInvalid;
					}

					if ((resultCode.equals(success)) || (resultCode.equals(passwordReset))
							|| (resultCode.equals(passwordExpiryReset)) || (resultCode.startsWith("1"))
							|| (resultCode.equals(passwordFirstLogin))) {
						SPersProfile persProfile = (SPersProfile) getPersProfile(persDetail.getPersId());

						sessionObj = setSessions(sessionObj, request, persDetail, persProfile, loginHandler);
						try {
							loginHandlerOperationsDAO.loginParams(persDetail, request, sessionObj);
						} catch (Exception e) {
						}
						if (concurrentStatus != null && !"".equalsIgnoreCase(concurrentStatus)) { // som
							if ("VM_ON_BOARD_REGISTRAR".equalsIgnoreCase(
									String.valueOf(request.getSession(false).getAttribute("ssRole")))) {
								resultCode = success;
							} else {
								resultCode = concurrentStatus;
							}

						}

						// System.err.println("concurrentStatus:::"+concurrentStatus);
						int loginAttempts = 0;
						if (personnel.getLoginAttempts() != null) {
							loginAttempts = personnel.getLoginAttempts().intValue();
						}

					}
				} else {
					// System.err.println("** User Not exist in S_PERSONNEL***");
					resultCode = userPasswordInvalid;
				}
			} else {
				// System.err.println("** User Not exist in S_PERS_DETAIL***");
				resultCode = userPasswordInvalid;
			}
			loginObj.put("resultCode", resultCode);
			loginObj.put("sessionObj", sessionObj);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return loginObj;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public String getOrgnPropertyvalue(String key, String domain, String orgnId) {
		String keyValue = "";
		String query = " SELECT PROPERTY_VALUE FROM C_ORGN_PROPERTIES" + " WHERE ORGN_ID=:ORGN_ID AND DOMAIN=:DOMAIN"
				+ " AND PROPERTY_NAME=:PROPERTY_NAME";
		HashMap<String, Object> inputMap = new HashMap<String, Object>();
		try {
			inputMap.put("ORGN_ID", orgnId);
			inputMap.put("DOMAIN", domain);
			inputMap.put("PROPERTY_NAME", key);
			List listPropval = access.sqlqueryWithParams(query, inputMap);
			if (!listPropval.isEmpty()) {
				keyValue = (String) listPropval.get(0);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return keyValue;
	}

	@Transactional
	public String getGeneratedLicenceKey(String orgnId) {
		String clientLicenceKey = "";
		try {
			String query = "SELECT CLIENT_KEY FROM S_CLIENT_LICENCE_AUTH WHERE ORGN_ID =:ORGN_ID ORDER BY SEQUENCE_NO DESC";
			Map<String, Object> selectMap = new HashMap<>();
			selectMap.put("ORGN_ID", orgnId);
			List<String> licenceKeyList = access.sqlqueryWithParamsLimit(query, selectMap, 1, 0);
			if (licenceKeyList != null && !licenceKeyList.isEmpty()) {
				clientLicenceKey = licenceKeyList.get(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return clientLicenceKey;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public Object getAuthorisation(String persId) {
		Object object = null;
		try {
			String query = " FROM SAuthorisation WHERE id.persId =:persId ";
			Map<String, Object> authMap = new HashMap<>();
			authMap.put("persId", persId);
			List authList = access.queryWithParams(query, authMap);
			if (authList != null && !authList.isEmpty()) {
				object = authList.get(0);
			} else {
				// logger.info("*** SAuthorisation Empty****");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return object;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public String checkActiveExpiry(SPersDetail persDetail, SPersonnel personnel, long dag, int warningTime) {

		Date currentDate = new Date();
		Date userExpDate;
		Date passExpDate;
		String active;
		try {
			userExpDate = persDetail.getExpiryDate();

			passExpDate = personnel.getExpiryDate();

			active = personnel.getStatus();

		} catch (Exception e) {
			e.printStackTrace();
			// this.differentError += ": There was an error checking the users expiry
			// dates.";
			return "error";
		}
		if ((active != null) && (active.equals("ACTIVE"))) {

			if ((userExpDate != null) && (!userExpDate.toString().isEmpty()) && (passExpDate != null)
					&& (!passExpDate.toString().isEmpty())) {
				userExpDate.setDate(userExpDate.getDate() + 1);// for ignoring current day
				passExpDate.setDate(passExpDate.getDate() + 1);// for ignoring current day
				if (!currentDate.after(userExpDate)) {
					if (!currentDate.after(passExpDate)) {
						System.err.println("user & Password Date Not Exceeded");
						return success;
					} else {
						return userPasswordExpiry2;
					}
				} else {
					return userPasswordExpiry;
				}
//                int userNameExpired = userExpDate.compareTo(currentDate);
//                int passwordExpired = passExpDate.compareTo(currentDate);
//                if (userNameExpired > 0) {
//                    if (passwordExpired > 0) {
//                        if (passExpDate.getTime() > currentDate.getTime() + dag * warningTime) {
//                            return success;
//                        }
////                        for (int x = warningTime - 1; x >= 0; x--) {
////                            if (passExpDate.getTime() > currentDate.getTime() + dag * x) {
////                                return "1" + x;
////                            }
////                        }
////                        return "0";
//                    }
//                    return userPasswordExpiry2;
//                }
//                return userPasswordExpiry;
			}
			return expiryInvalid;
		}
		return userInactive;

	}

	@Transactional
	public JSONObject featchLanguage(HttpServletRequest request, JSONObject sessionObj) {

		JSONObject langObj = new JSONObject();

		try {

			List mainlist = new ArrayList();
			SPersDetail persDetail = getPersDetailsByUsername((String) sessionObj.get("ssUsername"));

			if (persDetail != null) {

				String query = "from SPersOrgnLang where id.persId=:persId and id.orgnId=:orgnId";
				String query_lang_name = "from BLanguage where id.languageId=:languageId";

				Map<String, Object> map = new HashMap<>();
				map.put("persId", persDetail.getPersId());
				map.put("orgnId", (String) sessionObj.get("ssUserOrgId"));
				List list = access.queryWithParams(query, map);

				if (list != null && !list.isEmpty()) {

					for (int i = 0; i < list.size(); i++) {

						SPersOrgnLang persOrgnLang = (SPersOrgnLang) list.get(i);

						Map<String, Object> map_lang = new HashMap<>();
						map_lang.put("languageId", persOrgnLang.getId().getLanguageId());

						List langList = access.queryWithParams(query_lang_name, map_lang);

						JSONObject persOrgnLangObj = new JSONObject();
						if (langList != null && !langList.isEmpty()) {

							BLanguage bLanguage = (BLanguage) langList.get(0);

							persOrgnLangObj.put("languageId", persOrgnLang.getId().getLanguageId());
							persOrgnLangObj.put("locale", persOrgnLang.getId().getLocale());
							persOrgnLangObj.put("languageName", bLanguage.getId().getName());
							persOrgnLangObj.put("persId", persOrgnLang.getId().getPersId());
							persOrgnLangObj.put("orgnId", persOrgnLang.getId().getOrgnId());
							persOrgnLangObj.put("defaultFlag", persOrgnLang.getDefaultFlag());
							persOrgnLangObj.put("defaultFlag", persOrgnLang.getDefaultFlag());

						}

						mainlist.add(persOrgnLangObj);
					}

					langObj.put("langList", mainlist);

				}

			}

			request.getSession(false).setAttribute("langList", langObj);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return langObj;
	}

	@Transactional
	public JSONObject featchLanguage(String persId, String orgnId, String language) {

		JSONObject langObj = new JSONObject();

		try {

			List mainlist = new ArrayList();

			if (persId != null && !persId.equalsIgnoreCase("")) {

				String query = "from SPersOrgnLang where id.persId=:persId and id.orgnId=:orgnId and id.locale=:locale";
				String query_lang_name = "from BLanguage where id.languageId=:languageId";

				Map<String, Object> map = new HashMap<>();
				map.put("persId", persId);
				map.put("orgnId", orgnId);
				map.put("locale", language);
				List list = access.queryWithParams(query, map);

				if (list != null && !list.isEmpty()) {

					for (int i = 0; i < list.size(); i++) {

						SPersOrgnLang persOrgnLang = (SPersOrgnLang) list.get(i);

						Map<String, Object> map_lang = new HashMap<>();
						map_lang.put("languageId", persOrgnLang.getId().getLanguageId());

						List langList = access.queryWithParams(query_lang_name, map_lang);

						JSONObject persOrgnLangObj = new JSONObject();
						if (langList != null && !langList.isEmpty()) {

							BLanguage bLanguage = (BLanguage) langList.get(0);

							persOrgnLangObj.put("languageId", persOrgnLang.getId().getLanguageId());
							persOrgnLangObj.put("locale", persOrgnLang.getId().getLocale());
							persOrgnLangObj.put("languageName", bLanguage.getId().getName());
							persOrgnLangObj.put("persId", persOrgnLang.getId().getPersId());
							persOrgnLangObj.put("orgnId", persOrgnLang.getId().getOrgnId());
							persOrgnLangObj.put("defaultFlag", persOrgnLang.getDefaultFlag());
							persOrgnLangObj.put("defaultFlag", persOrgnLang.getDefaultFlag());

						}

						mainlist.add(persOrgnLangObj);

					}

					langObj.put("langList", mainlist);

				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return langObj;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public Object getPersProfile(String persId) {
		Object object = null;
		try {
			String query = " FROM SPersProfile where id.persId=:persId and defaultInd = 'Y' ";
			Map<String, Object> authMap = new HashMap<>();
			authMap.put("persId", persId);
			List authList = access.queryWithParamsWithLimit(query, authMap, 1, 0);
			if (authList != null && !authList.isEmpty()) {
				object = authList.get(0);
			} else {
				query = " FROM SPersProfile where id.persId=:persId ";
				authList = access.queryWithParamsWithLimit(query, authMap, 1, 0);
				if (authList != null && !authList.isEmpty()) {
					object = authList.get(0);
				}
				// logger.info("*** SAuthorisation Empty****");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return object;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public JSONObject setSessions(JSONObject sessionObj, HttpServletRequest request, SPersDetail persDetail,
			SPersProfile persProfile, LoginHandler loginHandler) {
		try {
			sessionObj.put("ssOrgId", request.getParameter("ssOrgId"));
//        sessionObj.put("ssValid", "Yes");
//        sessionObj.put("showHelp", "true");
//        sessionObj.put("ssTimeOut", session.getMaxInactiveInterval() + "");
			if (loginHandler != null && loginHandler.getLanguage() != null
					&& !"".equalsIgnoreCase(loginHandler.getLanguage())) {
				sessionObj.put("ssLocale", loginHandler.getLanguage());
				sessionObj.put("ssLanguageID", getLanguageIdByLocale(loginHandler.getLanguage(), ""));

			} else {
				sessionObj.put("ssLocale", persProfile.getId().getLocale());
				sessionObj.put("ssLanguageID", getLanguageIdByLocale(persProfile.getId().getLocale(), ""));
			}

			try {

				String role = "";
				if (persDetail != null) {
					sessionObj.put("ssUsername", persDetail.getUserName().toUpperCase());
					sessionObj.put("ssfname", persDetail.getFirstName());
					sessionObj.put("sslname", persDetail.getLastName());
					role = persProfile.getId().getRoleId();
					sessionObj.put("ssRole", role);

				} else {
					// logger.info("******* USER Not Exist S_PERS_DETAIL Empty*****");
				}

			} catch (Exception ex) {
				ex.printStackTrace();
			}
			sessionValues(sessionObj, request);
			String timeZone = getTimeZoneByLocale((String) sessionObj.get("ssLocale"),
					(String) sessionObj.get("ssOrgId"));
			String currentDomain = cloudUtills.getDomainByRole((String) sessionObj.get("ssRole"));
			sessionObj.put("ssDomain", currentDomain);
			sessionObj.put("ssTimeZone", timeZone);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return sessionObj;
	}

	@Transactional
	public String getLanguageIdByLocale(String locale, String orgnId) {
		String languageID = "";
		try {
			String languageIdQuery = " select id.languageId from BLanguage where countryCode =:countryCode and languageCode =:languageCode";
			Map<String, Object> languageMap = new HashMap<>();
			languageMap.put("languageCode", locale.split("_")[0]);
			languageMap.put("countryCode", locale.split("_")[1]);
			List langList = access.queryWithParams(languageIdQuery, languageMap);
			if (langList != null && !langList.isEmpty()) {
				languageID = (String) langList.get(0);
			}
			System.out.println("languageID:::" + languageID);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return languageID;
	}

	@Transactional
	public String getTimeZoneByLocale(String locale, String orgnId) {
		String timeZone = "";
		try {
			String timeZoneQuery = " select timeZone from BTimeZone where id.locale =:locale and id.orgnId =:orgnId";
			Map<String, Object> timeZoneMap = new HashMap<>();
			timeZoneMap.put("locale", locale);
			timeZoneMap.put("orgnId", orgnId);
			List timeZoneList = access.queryWithParams(timeZoneQuery, timeZoneMap);
			if (timeZoneList != null && !timeZoneList.isEmpty()) {
				timeZone = (String) timeZoneList.get(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return timeZone;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public JSONObject sessionValues(JSONObject sessionObj, HttpServletRequest request) {
		try {
			List sessionKeys = new ArrayList();
			if (sessionObj != null && !sessionObj.isEmpty()) {
				sessionKeys.addAll(sessionObj.keySet());
			}
			String query = " select id from BSessions where defaultFlag ='Y' order by sequenceNo";
			List sessionsList = access.queryWithParams(query, Collections.EMPTY_MAP);
			if (sessionsList != null && !sessionsList.isEmpty()) {
				for (int i = 0; i < sessionsList.size(); i++) {
					BSessionsId bSessionsId = (BSessionsId) sessionsList.get(i);
					if (bSessionsId != null) {
						sessionKeys.add(bSessionsId.getParameter());
						if (bSessionsId.getProcessValue() != null
								&& !"".equalsIgnoreCase(bSessionsId.getProcessValue().trim())
								&& !"null".equalsIgnoreCase(bSessionsId.getProcessValue().trim())) {
							if (bSessionsId.getProcessValue().toUpperCase().startsWith("SELECT")) {
//                                System.out.println("Query::B::" + bSessionsId.getProcessValue());
								String selectQuery = bSessionsId.getProcessValue();
								for (int j = 0; j < sessionKeys.size(); j++) {
									String sessionName = String.valueOf(sessionKeys.get(j));
									if (sessionName != null && !"".equalsIgnoreCase(sessionName)) {
										selectQuery = loginHandlerOperationsDAO.replaceingSessionValue(sessionName,
												selectQuery, sessionObj);
									}

								}
//                                System.out.println("Query::A::" + selectQuery);
								try {
									List dataList = access.sqlqueryWithParamsLimit(selectQuery, Collections.EMPTY_MAP,
											1, 0);
									if (dataList != null && !dataList.isEmpty()) {
										if (dataList.get(0) != null && dataList.get(0) instanceof byte[]) {
											byte[] bytesArray = (byte[]) dataList.get(0);
											String sesValue = new RAW(bytesArray).stringValue();
											if (sesValue != null && !"".equalsIgnoreCase(sesValue)) {
												sessionObj.put(bSessionsId.getParameter(), sesValue);
											} else {
												sessionObj.put(bSessionsId.getParameter(), "");
											}
										} else if (dataList.get(0) != null && dataList.get(0) instanceof Object[]) {
											Object[] sessionDataArray = (Object[]) dataList.get(0);
											if (sessionDataArray != null && sessionDataArray.length != 0) {
												sessionObj.put(bSessionsId.getParameter(), sessionDataArray[0]);
											}
										} else {
											sessionObj.put(bSessionsId.getParameter(), dataList.get(0));
										}
									} else {
										sessionObj.put(bSessionsId.getParameter(), "");
									}
								} catch (Exception e) {
									System.out.println("Problem in this Query :::" + selectQuery);
									continue;
								}

//                                sessionKeys.add(bSessionsId.getParameter());
							} else {
								sessionObj.put(bSessionsId.getParameter(), bSessionsId.getProcessValue());

							}
						}
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return sessionObj;
	}

	@Transactional
	public synchronized JSONObject getLogonDetails(HttpServletRequest request, String sessionId, String ssUsername) {

		JSONObject jSONObject = new JSONObject();
		try {
			SPersDetail details = getPersDetailsByUsername(ssUsername);
			sessionId = request.getSession(false).getId();
			List list = getAuditHistByPersId(details.getPersId(), sessionId);
//            List list = getAuditHistByPersId(details.getPersId(), ssUsername);

			if (list != null && !list.isEmpty() && list.size() > 0) {
				SPersAudit audit = (SPersAudit) list.get(0);
				jSONObject.put("ipAddress", audit.getIpAddress());
				jSONObject.put("deviceName", audit.getDeviceName());
				jSONObject.put("loginDate", new Timestamp(audit.getLoginDate().getTime()));
				jSONObject.put("userName", ssUsername);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jSONObject;
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
			list = access.queryWithParams(query, map);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return list;

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
	public int cloudLogout(String persId, String sessionId) {
		int updateCount = 0;
		try {
			String updateQuery = "UPDATE S_PERS_AUDIT SET LOGOUT_DATE =:LOGOUT_DATE, FLAG =:FLAG WHERE PERS_ID =:PERS_ID AND SESSION_ID =:SESSION_ID";
			Map<String, Object> updateMap = new HashMap<>();
			updateMap.put("FLAG", "Y");
			updateMap.put("LOGOUT_DATE", new Date());
			updateMap.put("PERS_ID", persId);
			updateMap.put("SESSION_ID", sessionId);
			System.out.println("updateQuery:::" + updateQuery);
			System.out.println("updateMap:::" + updateMap);
			updateCount = access.executeUpdateSQLNoAudit(updateQuery, updateMap);
			System.out.println("updateCount:::" + updateCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return updateCount;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public String getPersnolisationImageUrl(String persId) {
		String result = "";
		try {
			String listQuery = "SELECT CONTENT FROM S_PERSNOLISATION WHERE PERS_ID =:PERS_ID";
			Map<String, Object> listMap = new HashMap<>();
			listMap.put("PERS_ID", persId);
			List queryList = access.sqlqueryWithParams(listQuery, listMap);
			if (queryList != null && !queryList.isEmpty()) {
				Blob blobData = (Blob) queryList.get(0);
				if (blobData != null) {
					Long blob_len = blobData.length();
					byte[] byteArray = new byte[blob_len.intValue()];
					blobData.getBinaryStream().read(byteArray);
					String encodedString = Base64.getEncoder().encodeToString(byteArray);
					result = "data:image/png;base64," + encodedString;
				}
			} else {
				result = "images/Home-Iocn.svg";
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public List getTwitterData(HttpServletRequest request) {
		List resultList = new ArrayList();
		try {
			String query = "SELECT USER_SCREEN_NAME, TRANSLATED_TWEET, TWEET_URL, LIKES_COUNT, RETWEET_COUNT, USER_FOLLOWERS, PUBLISHED_DATE,TWEET_ID FROM TW_BY_LEADERS_TIMELINE ORDER BY CREATE_DATE DESC FETCH FIRST 10 ROWS ONLY";
			// + "WHERE USER_SCREEN_NAME=:USER_SCREEN_NAME";
			Map queryMap = new HashMap<>();
			// queryMap.put("USER_SCREEN_NAME", "PiLogGroup");
			resultList = access.sqlqueryWithParams(query, queryMap);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultList;
	}

	@Transactional
	public int getCountBasedOnDomainName(HttpServletRequest request, String query) {
		int count = 0;
		try {

			List licenceKeyList = access.sqlqueryWithParams(query, Collections.EMPTY_MAP);
			if (licenceKeyList != null && !licenceKeyList.isEmpty()) {
				count = cloudUtills.convertIntoInteger(licenceKeyList.get(0));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return count;
	}

	public List getAVGFeedbackRating(HttpServletRequest request) {

		List resultList = new ArrayList();
		try {
			String labelsQuery = "SELECT FEEDBACK, AVG(RATING) AS RATING,COUNT(EMAIL) AS CNT FROM O_FEEDBACK GROUP BY FEEDBACK";
			resultList = access.sqlqueryWithParams(labelsQuery, Collections.EMPTY_MAP);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultList;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public JSONObject getNIICSaveFeedbackForm(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String result = "";
			int count = 0;
			String dataToSend = request.getParameter("dataToSend");
			if (dataToSend != null && !"".equalsIgnoreCase(dataToSend) && !"null".equalsIgnoreCase(dataToSend)) {
				JSONObject dataObj = (JSONObject) JSONValue.parse(dataToSend);
				if (dataObj != null && !dataObj.isEmpty()) {
					long ratingCnt = 0;
					ratingCnt = (long) dataObj.get("ratingCount");
					if (ratingCnt > 0) {
						String userName = (String) dataObj.get("name");
						String userEmail = (String) dataObj.get("email");
						for (int i = 0; i < ratingCnt; i++) {
							String dataValue = (String) dataObj.get("rating" + i);
							if (dataValue != null && !"".equalsIgnoreCase(dataValue)) {
								String feedBackLabel = dataValue.split(":")[0];
								String feedBackvalue = dataValue.split(":")[1];
								Map calenderMap = new HashMap();
								String insertSubQuery = "INSERT INTO O_FEEDBACK(FEEDBACK_ID, FEEDBACK, RATING, USERNAME, "
										+ "EMAIL, CREATE_DATE) VALUES (?,?,?,?,?,?)";
								calenderMap.put(1, AuditIdGenerator.genRandom32Hex());
								calenderMap.put(2, feedBackLabel);
								calenderMap.put(3, feedBackvalue);
								calenderMap.put(4, userName);
								calenderMap.put(5, userEmail);
								calenderMap.put(6, new Date());
								count += access.executeNativeUpdateSQLWithSimpleParamsNoAudit(insertSubQuery,
										calenderMap);

							}
						}
					}

				}
				if (count > 0) {
					result = "Thanks for your feedback";
					resultObj.put("result", result);
					resultObj.put("resultFlag", true);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public List getEvents(HttpServletRequest request, List dateList) {
		List resultList = new ArrayList();
		try {
			String calenderDataQuery = "";
			if (dateList != null && !dateList.isEmpty()) {
				String str = (String) dateList.stream().map(String::valueOf).collect(Collectors.joining("','"));
				calenderDataQuery = "SELECT EVENT_TITLE, EVENT_DESC,EVENT_DATE FROM SMART_CALENDER WHERE EVENT_DATE IN ('"
						+ str + "') ORDER BY EVENT_DESC ASC";
			}
			resultList = access.sqlqueryWithParams(calenderDataQuery, new HashMap());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultList;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public JSONObject setuserProfileUpdateIcon(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String imgUrl = request.getParameter("imgURL");
			imgUrl = imgUrl.substring(imgUrl.indexOf(',') + 1);
			byte[] imgByte = Base64.getDecoder().decode(imgUrl);
			Blob imgBlob = new SerialBlob(imgByte);
			String loggedInUser = (String) request.getSession(false).getAttribute("ssUsername");
			String fileName = request.getParameter("fileName");
			SPersDetail persDetail = getPersDetailsByUsername(loggedInUser);
			Set<SPersnolisation> sPersSet = persDetail.getSPersnolisations();
			if (!sPersSet.isEmpty() && sPersSet != null) {
				SPersnolisation sPersnolisation = sPersSet.stream().findFirst().get();
				access.delete(sPersnolisation);
				SPersnolisation persnolisation = new SPersnolisation();
				SPersnolisationId id = new SPersnolisationId();
				id.setPersId(persDetail.getPersId());
				id.setFileName(fileName);
				id.setType("P");
				persnolisation.setContent(imgBlob);
				persnolisation.setAuditId(AuditIdGenerator.getAuditId("S_PERSNOLISATION"));
				persnolisation.setCreateBy((String) request.getSession(false).getAttribute("ssUsername"));
				persnolisation.setEditBy((String) request.getSession(false).getAttribute("ssUsername"));
				persnolisation.setCreateDate(cloudUtills
						.getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
				persnolisation.setEditDate(cloudUtills
						.getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
				persnolisation.setId(id);
				this.access.save(persnolisation);
			} else {
				SPersnolisation persnolisation = new SPersnolisation();
				SPersnolisationId id = new SPersnolisationId();
				id.setPersId(persDetail.getPersId());
				id.setFileName(fileName);
				id.setType("P");
				persnolisation.setContent(imgBlob);
				persnolisation.setAuditId(AuditIdGenerator.getAuditId("S_PERSNOLISATION"));
				persnolisation.setCreateBy((String) request.getSession(false).getAttribute("ssUsername"));
				persnolisation.setEditBy((String) request.getSession(false).getAttribute("ssUsername"));
				persnolisation.setCreateDate(cloudUtills
						.getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
				persnolisation.setEditDate(cloudUtills
						.getCurrentTimeZoneData((String) request.getSession(false).getAttribute("ssTimeZone")));
				persnolisation.setId(id);
				this.access.save(persnolisation);
			}
			resultObj.put("flag", Boolean.valueOf(true));
			resultObj.put("message", "Profile image added successfully");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public String removeUserProfilePic(HttpServletRequest request) {
		String ssUsername = request.getParameter("userName");
		String result = "";
		try {
			String query = "DELETE FROM S_PERSNOLISATION WHERE PERS_ID=(SELECT PERS_ID FROM S_PERS_DETAIL WHERE USER_NAME ='"
					+ ssUsername + "')";
			Integer records = Integer.valueOf(access.executeUpdateSQL(query));
			if (records.intValue() > 0) {
				result = " Profile Image Deleted Successfully.";
			} else {
				result = " Failed to Delete Profile Image.";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void updateLogOutUserNameOtherSessionIds(String userName, HttpServletRequest request) {
		try {
			SPersDetail details = getPersDetailsByUsername(userName.toUpperCase());
			String sessionId = request.getSession(false).getId();
			List<SPersAudit> list = getAuditHistByPersId(details.getPersId(), sessionId);
			updateLogOutByAllPersIdOtherSessionIds(details.getPersId(), sessionId);
			if (list != null && !list.isEmpty()) {
				Map sessionsMap = HttpSessionListenerPublisher.sessions;
				for (int i = 0; i < list.size(); i++) {
					SPersAudit persAudit = list.get(i);
					if (!sessionId.equalsIgnoreCase(persAudit.getId().getSessionId())) {
						HttpSession sessionOld = (HttpSession) sessionsMap.get(persAudit.getId().getSessionId());
						if (sessionOld != null)
							try {
								sessionOld.invalidate();
							} catch (Exception ex) {
								ex.printStackTrace();
							}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public int updateLogOutByAllPersIdOtherSessionIds(String persId, String sessionId) {
		int updateCount = 0;
		try {
			String updateQuery = "UPDATE S_PERS_AUDIT SET FLAG =:FLAG ,LOGOUT_DATE=:LOGOUT_DATE WHERE PERS_ID =:PERS_ID  AND SESSION_ID <> :SESSION_ID  AND FLAG ='N' ";
			Map<String, Object> updateMap = new HashMap<>();
			updateMap.put("FLAG", "Y");
			updateMap.put("LOGOUT_DATE", new Date());
			updateMap.put("PERS_ID", persId);
			updateMap.put("SESSION_ID", sessionId);
			updateCount = this.access.executeUpdateSQLNoAudit(updateQuery, updateMap);
			System.out.println("updateCount::updateLogOutByAllPersId::" + updateCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return updateCount;
	}

}
