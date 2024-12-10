/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
var shakeOptions = { times: 10, direction: "left", distance: 30 };
var ssDatePickerObj = {};
var grioldDataObj = {};
var selecteIndexes = [];
var globalTabId;
var testInterval;
var labelObject = {};
var initialTblViewData = "";
var accordionSwitchflag = true;
var tabSwitchflag = true;
var changeflag = false;
var tabSwitched = true;
var tabsOldData = {};
var oldClickedValue = "";
var currentRole = "";
$(document).ready(function() {
	$("#roleSecure").hide();
	$('#loginModel').keyup(function(event) {
		if (event.which === 13) {
			event.preventDefault();
			loginOpeartions();
		}
	});
	$("#loginModel").on("hidden.bs.modal", function() {
		$("#loginError").html("");
		$("#rsUsername").val("");
		$("#rsPassword").val("");
	});

	$('#toggleloginDiv').on('click', function() {
		$('#visionLoginpageInner').toggle();
	});

});
function loginOpeartions() {
	$("#loginError").hide();
	var rsUsername = $("#rsUsername").val();
	var rsPassword = $("#rsPassword").val();
	if (rsUsername != null && rsUsername != '' && rsUsername != undefined) {
		$("#isPasswordShowHide").show();
		$("#isUsernameShowHide").hide();
		$("#isNextBtnShowHide").hide();
		$("#isSigninBtnShowHide").show();
		$("#processLoginID2").show();
		$("#isFrgtPwedBtnShowHide").show();
		$("#afterUserFlag").val('Y');
		$("#rsUsername").focus();

	}
	if (!(rsUsername != null && rsUsername != '')) {
		$("#loginError").html("Username cannot be empty");
		$("#loginError").show();
		//        $(".visionLoginpageInner").effect("shake", shakeOptions);

	} else {
		$("#rsPassword").focus();
		$("#loginError").hide();
		if (rsUsername != null && rsUsername != '' &&
			rsPassword != null && rsPassword != '') {
			processLoginSubmit();
		}
	}
}
function loginOpeartions2() {


	var rsPassword = $("#rsPassword").val();
	var rsUsername = $("#rsUsername").val();
	if (!(rsUsername != null && rsUsername != "")) {
		//        $(".visionLoginpageInner").effect("shake", shakeOptions);
		$("#rsUsername").focus();
		//        $("#processLoginID2").hide();
		$("#processLoginID").show();
		$("#loginError").show();
		var blinkText = "<blink>Username cannot be empty</blink>";
		$("#loginError").html(blinkText);
		$("#roleSecure").hide();
		$("#rsPassword").val('');
		$("#loginError").css("color", "red");

	} else if (!(rsPassword != null && rsPassword != "")) {
		$("#passwordError").show();
		$("#loginError").hide();
		var blinkText = "<blink>Password cannot be empty</blink>";
		$("#passwordError").html(blinkText);
		//        $(".visionLoginpageInner").effect("shake", shakeOptions);
		$("#rsPassword").focus();
		$("#passwordError").css("color", "red");
		//        setInterval(function () {
		//            blinkText.style.opacity = (blinkText.style.opacity == 0 ? 1 : 0);
		//        }, 1500);
	} else {
		$("#loginError").hide();
		//        $("#processLoginID2").hide();
		$("#processLoginID").show();
		processLoginSubmit();
		//        var  showLoader = setTimeout("$('#Loader').show()", 300);
	}
}


function closeLoginForm() {
	$("#roleSecure").hide();
}
function processLoginSubmit() {
	$("#Loader").css("display", "block");
	$("#wait").css("display", "block");
	var userName = $("#rsUsername").val();
	$("#loginError").hide();
	var secretKey = $('meta[name=keygeneration]').attr("content");
	var rsPassword = $("#rsPassword").val();
	var encryptedPassword = CryptoJS.AES.encrypt(rsPassword, secretKey);
	$("#rsPasswordHid").val(encryptedPassword);
	//    console.log(encryptedPassword);
	$("#rsPassword").attr("disabled", "disabled");
	var rsPassword = $("#rsPassword").val();
	var currentPageURL = $("#currentPageUrl").val();
	var data = {};
	$("#visionLoginpageInner :input").each(function() {
		var textid = $(this).attr("id");
		var type = $(this).attr("type");
		var inputName = $(this).attr("name");
		var textval = $(this).val();
		if (type != 'hidden') {
			if (textval != null && textval != '') {
				textval = textval;
			}
		}
		if (textid != null && textid != 'CREATE_DATE') {
			data[inputName] = textval;
		}
	});
	if (data != null && !jQuery.isEmptyObject(data)) {
	    data['timezone']= Intl.DateTimeFormat().resolvedOptions().timeZone;
		$.ajax({
			datatype: "json",
			type: "post",
			url: "cloudLogin",
			data: data,
			traditional: true,
			cache: false,
			success: function(response) {
				stopLoader();
				if (response != null && !jQuery.isEmptyObject(response)) {
					var loginSession = response['loginSession'];
					localStorage.setItem("loginSession", loginSession);
					localStorage.setItem("userName", userName);
					localStorage.removeItem("jsessionId");
					var returnCde = response['returnCde'];
					var role = response['role'];
					//                    var sessionObj = response['sessionObj'];
					sessionStorage.setItem("currentRole", response['role']);
					//                    $("#result").val(sessionObj);
					console.log("returnCde:::" + returnCde);
					if (returnCde != null && returnCde != '') {
						if (response['profile_imgStr'] != 'No_data') {
							localStorage.setItem("profile_imgStr", response['profile_imgStr']);
							//                            sessionStorage.setItem("profile_imgStr", 'images/no-image.jpg');
						} else {
							localStorage.setItem("profile_imgStr", 'images/Profile_Icon.svg');
						}
						if (returnCde.indexOf("success") > -1) {
							if (currentPageURL != null && currentPageURL != '' &&
								currentPageURL != undefined && currentPageURL != "null") {
								navigationMenuUrl(currentPageURL);
								$('#chatBotIcon').attr('data-flag', 'A');
							} else {
								navigationMenuUrl("homePage?ssUsername='" + response['ssUsername'] + "'");
								$("#Loader").css("display", "none");
								$('#chatBotIcon').attr('data-flag', 'A');
								//                                window.location.href = 'homePage';
							}

						} else if (returnCde.indexOf("alreadyLoggedIn") > -1) {//alreadyLoggedIn
							var modalObj = {
								title: 'Message',
								body: response['htmlStr']
							};
							var buttonArray = [
								{
									text: 'Ok',
									"class": 'dialogyes',
									click: function() {
										//navigationMenuUrl("homePage?ssUsername='" + response['ssUsername'] + "'"); umar
										navigationMenuUrl("homePage?ssUsername=" + response['ssUsername'] + "&terminate=1");
										$("#Loader").css("display", "none");
										$('#chatBotIcon').attr('data-flag', 'A');////update flag


									},
									isCloseButton: true
								},
								{
									text: 'Cancel',
									"class": 'dialogno',
									click: function() {
										navigationMenuUrl("cloudLogout");
										$('#chatBotIcon').attr('data-flag', 'I');////update flag

									},
									isCloseButton: true
								}
							];
							modalObj['buttons'] = buttonArray;
							//not showing already logged in popup directly we are calling the next url
							//createModal("visionLoginpageInner", modalObj);  
							navigationMenuUrl("homePage?ssUsername=" + response['ssUsername'] + "&terminate=1");
						} else {
							//                            $(".visionLoginpageInner").effect("shake", shakeOptions);
							$("#loginError").html(response['errorMesg']);
							$("#rsPassword").removeAttr("disabled");
							$("#rsPassword").val("");
							$("#rsUsername").val("");
							$("#isUsernameShowHide").show();
							$("#isPasswordShowHide").hide();
							$("#loginError").show();
							$("#isNextBtnShowHide").show();
							$("#isFrgtPwedBtnShowHide").hide();
						}
					}
				}
			},
			error: function(e) {
				ajaxStop();
				alert('Error: ' + e);

			}
		});
	}
}
function cloudUserRegister(basicData, currentIndex) {
	ajaxStart();
	if (basicData != null && basicData != undefined) {
		$.ajax({
			datatype: "json",
			type: "POST",
			url: "register",
			traditional: true,
			cache: false,
			data: {
				basicData: JSON.stringify(basicData),
				currentIndex: currentIndex,
				ssOrgId: "C1F5CFB03F2E444DAE78ECCEAD80D27D",
				ssRole: "CLOUD",
				ssLocale: "en_US"
			},
			traditional: true,
			cache: false,
			success: function(response) {
				ajaxStop();
				if (response != null && !jQuery.isEmptyObject(response)) {
					var modalObj = {
						title: 'Message',
						body: response['Message']
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {
								if (response['MessageFlag']) {
									window.location.href = 'homePage';
									//                                    window.location.href = "https://pilogcloud.com/iVisionDXP/";
								} else {
									$("#myModal").css("display", "none");
								}
							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("myModal", modalObj);
				}
			},
			error: function(e) {
				ajaxStop();
				alert('Error: ' + e);
			}
		});
	}
}
$(document).ready(function() {
	$.validator.addMethod('strongPassword', function(value, element) {
		return this.optional(element)
			|| value.length >= 8
			&& /\d/.test(value)
			&& /[a-z]/i.test(value)
			&& /[A-Z]/g.test(value)
			&& /[0-9]/g.test(value)
			&& /[=!\-@._*\$\#\%\^\&\(\)\~\`\<\>\/\?\\\|\{\}\[\]\;\:\'\"\,\+]/.test(value);
	});
	$("#ValidateResetPassword").validate({
		rules: {
			old_password: {
				required: true
			},
			new_password: {
				required: true,
				strongPassword: true,
			},
			confirm_password: {
				required: true,
				equalTo: "#new-password"
			}
		},
		messages: {
			old_password: {
				required: "Please provide an oldpassword",
				strongPassword: "Password must contain atleast 8 characters length and a number, a special character, a lowercase and a uppercase letter"
			},
			new_password: {
				required: "Please provide an new password",
				strongPassword: "Password must contain atleast 8 characters length and a number, a special character, a lowercase and a uppercase letter"
			},
			confirm_password: {
				required: "Please provide confirm password",
				equalTo: "New password and confirm password must be same"
			},
		}
	});
});

function changepassword() {
	ajaxStart();
	$("#ValidateResetPassword").valid();
	var oldPassword = $("#old-password").val();
	var pwd = $("#new-password").val();
	var password2 = $("#confirm-password").val();
	var secretKey = $('meta[name=keygeneration]').attr("content");
	if (!oldPassword) {
		return false;
	} else if (!pwd) {
		return false;
	} else if (!password2) {
		return false;
	} else if (oldPassword == pwd) {
		return false;
	} else if (pwd == password2) {
		var oldPassword = $("#old-password").val();
		var encryptedOldPassword = CryptoJS.AES.encrypt(oldPassword, secretKey);
		$("#old-password").val(encryptedOldPassword);

		var password = $("#new-password").val();
		var encryptedPassword = CryptoJS.AES.encrypt(password, secretKey);
		$("#new-password").val(encryptedPassword);

		var confirm_password = $("#confirm-password").val();
		var encryptedConfirmPassword = CryptoJS.AES.encrypt(confirm_password, secretKey);
		$("#confirm-password").val(encryptedConfirmPassword);
		$('#wait').show();
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'json',
			url: 'changepassword',
			data: {
				old_password: $("#old-password").val(),
				password: $("#new-password").val(),
				confirm_password: $("#confirm-password").val()

			},
			cache: false,
			success: function(response) {
				ajaxStop();
				$('#wait').hide();
				if (response != null && !jQuery.isEmptyObject(response)) {
					var modalObj = {
						title: 'Message',
						body: response['message']
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {
								if (response['messageFlag']) {
									window.location.href = "cloudLogout";
								} else {
									$("#passWordErrorShowModal").css("display", "none");
								}
							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("passWordErrorShowModal", modalObj);
				}

			},
			error: function(e) {
				//  //alert(e.message)
				sessionTimeout(e);
			}

		});

	}
}
function genericpasswordvalidation(ele) {

	var ele = ele;
	var str = $("#" + ele).val();
	var errorID = "#dis_" + ele;
	var regex;
	var desc;
	var patt;
	var dataRegex = $("#" + ele).attr("data-pswdRegex");
	var dataDesc = $("#" + ele).attr("data-pswdDesc");
	var user_name = $("#user_name").val();
	if (!(user_name != null && user_name != '' && user_name != undefined)) {
		user_name = $("#userNameText").val();
	}
	if (str != null && str != '') {
		if (user_name != null && user_name != '' && user_name.toUpperCase() != str.toUpperCase()) {
			var pswdRegex = dataRegex.split(":::");
			var pswdDesc = dataDesc.split(":::");
			for (var i = 0; i < pswdRegex.length; i++) {
				regex = pswdRegex[i];
				patt = new RegExp(regex);

				if (!patt.test(str)) {
					var msg = pswdDesc[i];
					err_msg(errorID, msg);
					return false;
				} else {
					$(errorID).hide();
					$("#dis_pwd").html("");
					$("#password2").prop('disabled', false);
					//            $("#restpassword").prop('disabled', true);
				}
			}
			return true;
		} else {
			msg = "Password & Username should not match.";
			$("#myModal .registerMsg").append(msg);
			return false;
		}
	} else {
		msg = "Password should not blank.";
		$("#myModal .registerMsg").append(msg);
		return false;
	}

}
$(document).ready(function() {

	$("#ValidateForgotPassword").validate({
		rules: {
			User_Name: {
				required: true
			},
			E_Mail: {
				required: true,
			},
			Contact_Number: {
				required: true
			}


		},
		messages: {
			User_Name: {
				required: "Please provide an username"
			},
			E_Mail: {
				required: "Please provide an email"
			},
			Contact_Number: {
				required: "Please provide a contact number",
			},

		}
	});
});
function validate() {
	$("#ValidateForgotPassword").valid();
	ajaxStart();
	var email_id = $("#email_id").val();
	var user_id = $("#user_id").val();
	var contact_number = $("#contact_number").val();
	if (!user_id) {
		return false;
	} else
		if (!email_id) {
			return false;
		} else
			if (!contact_number) {
				return false;
			} else {
				$.ajax({
					type: "post",
					traditional: true,
					dataType: 'json',
					url: 'forgotPassword',
					data: {
						email_id: $("#email_id").val(),
						user_id: $("#user_id").val(),
						contact_number: $("#contact_number").val()
					},
					cache: false,
					success: function(result) {
						// //alert(result);
						ajaxStop();
						$('#wait').hide();
						var modalObj = {
							title: 'Message',
							body: result['response']
						};
						var buttonArray = [
							{
								text: 'Close',
								click: function() {
									window.location.href = 'homePage';
									//                            $('#loginModel').modal('show');
									$("#passWordErrorShowModal").css("display", "none");

								},
								isCloseButton: true
							}
						];
						modalObj['buttons'] = buttonArray;
						createModal("passWordErrorShowModal", modalObj);
					},
					error: function(e) {
						sessionTimeout(e);
					}

				});
			}
}
$(document).ready(function() {
	$("#updateProfile").validate({
		rules: {
			username: {
				required: true,

			},
			dob: {
				required: true,
			},
			mobile: {
				required: true
			},
			email: {
				required: true
			},
			country: {
				required: true
			},
			Address: {
				required: true
			}
		},
		messages: {
			username: {
				required: "Please provide username"
			},
			email: {
				required: "Please provide email-Id"
			},
			dob: {
				required: "Please provide date of birth"
			},
			mobile: {
				required: "Please provide mobile number"
			},
			country: {
				required: "Please provide country"
			},
			Address: {
				required: "Please provide address"
			}
		}
	})
});

function submitData() {
	ajaxStart();
	$("#updateProfile").valid();
	var data = userprofileValidation();
	//  alert("alert"+data);
	if (data) {

		var jsonOBJ = {};
		jsonOBJ.ids = [];
		jsonOBJ.values = [];
		$(".profile-form :input").each(function() {
			var textid = $(this).attr("id");
			var textval = $(this).val();
			jsonOBJ.ids.push(textid);
			jsonOBJ.values.push(textval);

		});

		$.ajax({
			traditional: true,
			dataType: 'html',
			type: 'POST',
			url: 'updateProfileData',
			cache: false,
			data: {
				jsonData: JSON.stringify(jsonOBJ)
			},
			success: function(response) {
				ajaxStop();
				var modalObj = {
					title: 'Message',
					body: "Updated Successfully"
				};
				var buttonArray = [
					{
						text: 'Close',
						click: function() {
							//                                submitData();
							window.location.href = 'userProfile';
						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("profileShowModal", modalObj);
			},
			error: function(e) {
				if (e.status == 1) {
					sessionTimeout(e);
				} else {
					var modalObj = {
						title: 'Message',
						body: "Failed to update"
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {
								window.location.href = 'userProfile';
								$("#profileShowModal").css("display", "none");

							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("profileShowModal", modalObj);
				}
			}



		});

		console.log(JSON.stringify(jsonOBJ));
	}
}
var UsrErrRslt = "";
function userdataValidation() {
	//   //alert("hi");
	var fname = $("#first_name").val();   // firstname
	UsrErrRslt = "";
	var lname = $("#last_name").val();    //lastname
	var mnumber = $("#mobile_number").val();  //mobilenumber
	var usremail = $("#email_id").val();   //email
	var hidden_usremail = $("#hidden_email_id").val();   //email
	var doy = $(".dateselector-year option:selected").val();  //dateofbirth
	var dom = $(".dateselector-month option:selected").val();  //dateofbirth
	var dod = $(".dateselector-day option:selected").val();  //dateofbirth
	var uname = $("#user_name").val();      //username
	var upass = $("#pwd").val();         //password
	var cpass = $("#password2").val();       //confirmpassword
	var uReg = $("#regionTxt").val();         //region
	var uLcle = $("#usr_lcle").val();         //locale
	var uRle = $("#usr_rle").val();           //role
	var uRprt = $("#usr_rpt").val();         //report to
	var uPlnt = $("#usr_plnt").val();        //plant  
	//  var uTelNo_a = $("#tel_no_a").val();        //Country code --additional data  
	var uTelNo_c = $("#tel_no_c").val();        //STD--additional data
	var uTelNo_b = $("#phone_number").val();        //phone_number--additional data
	var purposeofReg = $("#purposeofReg").val();        //phone_number--additional data


	var Trim_captch = $("#Usercaptch").val();  //To remove space in between
	var charPos = Trim_captch.charAt(4);
	if (charPos == " ") {
		var res = Trim_captch.replace(charPos, "");
		var ucptch = res;
		// //alert(ucptch);
	} else {
		var ucptch = Trim_captch;
	}
	var ccptch = $("#Matchcaptch").val();
	var uOrgId = $("#usr_orgid").val();


	var alphaNumeric = /^[a-zA-Z0-9&()\s]+$/;
	var alpha = /^[a-zA-Z\s]+$/;
	var nameAlpha = alpha.test(fname);
	var name1Alpha = alpha.test(lname);


	var mobileNo = /^\d{10}$/;
	var mobUsr = mobileNo.test(mnumber);
	var mobzero = /^[0]{1,}$/;


	var mail = /^[a-zA-Z0-9_.-]+@[a-zA-Z0-9.]([a-zA-Z0-9.]{1,})+\.[a-zA-Z0-9.]{2,}$/;
	var PiLogmail = /^[a-zA-Z0-9_.-]+@(pilog)+\.[a-zA-Z0-9.]{2,}$/;
	var emailUsr = mail.test(usremail);
	var pilogUsr = PiLogmail.test(usremail);


	var passwrd = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\s).{8,15}$/;
	var upasswrd = passwrd.test(upass);
	var cnfrmpss = passwrd.test(cpass);

	if (!fname) {
		return false;
	}
	$("#dis_first_name").hide();

	if (!lname) {
		return false;
	}
	$("#dis_last_name").hide();
	if (fname && (nameAlpha == false)) {
		return false;
	}
	$("#dis_first_name").hide();
	if (lname && (name1Alpha == false)) {
		return false;
	}
	$("#dis_last_name").hide();
	if (!uname) {
		return false;
	}
	$("#dis_user_name").hide();

	if (!uReg) {
		return false;
	}
	$("#dis_region").hide();
	if (!uRle) {
		return false;
	}
	$("#dis_usr_rle").hide();
	if (uRle != "FUNCT_CONSULTANT") {
		if (!upass) {
			return false;
		}
		$("#dis_pwd").hide();
		if (upass && (upasswrd == false)) {
			return false;
		}

		$("#dis_pwd").hide();
		if (!cpass) {
			return false;
		}
		$("#dis_password2").html("");
		if (upass != cpass) {
			return false;
		}

		$("#dis_password2").hide();

	} else {
		$("#password_star").hide();
		$("#pwd").html("disable", "disabled");
		$("#password2_star").hide();
		$("#password2").html("disable", "disabled");
	}

	/////////////////  

	if (!uLcle) {
		return false;
	}
	$("#dis_usr_lcle").hide();
	if (!uOrgId) {
		return false;
	}
	$("#dis_usr_orgid").hide();

	/*updated by ramu Start */
	if ((uRle.indexOf('VM_', 0) != 0) && (uRle.indexOf('CM_', 0) != 0)) {
		if (!uPlnt) {
			return false;
		}
		$("#dis_usr_plnt").hide();
	} else {
		$("#dis_usr_plnt").html("");
	}
	/*updated by ramu end */

	if (uRle.lastIndexOf("REQUESTOR") > -1) {
		if (!uRprt) {
			return false;
		}
		$("#dis_usr_rpt").hide();
	} else {
		$("#dis_usr_rpt").html("");
		//   return true;
	}

	if ((doy == "") || (dom == "") || (dod == "")) {
		return false;
	}
	$("#dis_dateofbirth").hide();


	if (!ucptch) {
		$("#dis_Usercaptch").show();
		$("#dis_Usercaptch").html("Should Not Be Null");
		return false;
	}

	if (ucptch && (ucptch == ccptch)) {
		$("#dis_Usercaptch").html("");

	} else {
		$("#dis_Usercaptch").html("Captch do not match.");
		$("#dis_Usercaptch").show();
		return false;

	}

	if (!usremail) {
		return false;
	}
	$("#dis_email_id").hide();
	if (uRle != "FUNCT_CONSULTANT") {
		if (usremail && (emailUsr == false)) {
			return false;
		} else {
			$("#dis_email").html("");
		}


	} else {
		if (usremail && (pilogUsr == false)) {
			return false;

		} else {
			$("#dis_email").html("");
		}
	}

	if (usremail != hidden_usremail) {
		// //alert("hiii");
		$("#dis_email_id").hide();
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'html',
			url: 'emailIdValidate',
			data: {
				email_id: $("#email_id").val()
			},
			cache: false,
			success: function(result) {

				////alert(result);
				// //alert(result);

				if (result == 'false') {

					var id = "#dis_email_id";
					var msg = "Email Already Exist.";
					err_msg(id, msg);
					return false;

				}
			},
			error: function(e) {
				//  //alert(e.message)
				sessionTimeout(e);
			}

		});
	}

	if (mnumber && (mobUsr == false)) {
		return false;
	}

	if (mobzero.test(mnumber) == true) {
		return false;
	}

	$("#dis_mobile_number").hide();

	if ((uTelNo_b && !uTelNo_c) || (!uTelNo_b && uTelNo_c)) {
		var id = ".dis_tel_no_c";

		if (!uTelNo_b) {
			msg = "Enter Telephone number";
		} else {

			msg = "Enter STD Code";
		}
		return false;
	}

	if (uTelNo_b && uTelNo_c) {

		var id = ".dis_tel_no_c";

		var StdZero = /^[0]{1,}$/;
		;
		var StdRg3 = /^[0-9]{3,5}$/;
		var res = StdRg3.test(uTelNo_c);
		if (StdZero.test(uTelNo_c) == true) {
			return false;
		} else {
			$(id).hide();
		}
		if (res == false) {
			return false;
		} else {
			$(id).hide();
		}
		//////////////////tel no

		var TelReg = /^[0-9]{6,8}$/;
		var res = TelReg.test(uTelNo_b);
		var TelZero = /^[0]{1,}$/;
		if (TelZero.test(uTelNo_b) == true) {
			return false;
		} else {
			$(id).hide();
		}

		if (res == false) {
			return false;
		} else {
			$(id).hide();

		}

	}
	//////////////////
	if ($('#agree').is(':checked') == false) {
		return false;
	}

	return true;

}
function userprofileValidation() {
	var usremail = $("#email_id").val();
	var hidden_usremail = $("#hidden_email_id").val();
	var user_name = $("#user_name").val();
	var dob = $("#dob").val();
	var address = $("#address").val();
	var country = $("#country").val();
	var mobileno = $("#mobileno").val();
	if (!usremail) {
		return false;
	} else if (!user_name) {
		return false;
	} else if (!dob) {
		return false;
	} else if (!address) {
		return false;
	} else if (!country) {
		return false;
	} else if (!mobileno) {
		return false;
	}

	$("#dis_email_id").hide();
	if (usremail != hidden_usremail) {
		$("#dis_email_id").hide();
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'html',
			url: 'emailIdValidate',
			data: {
				email_id: $("#email_id").val()
			},
			cache: false,
			success: function(result) {
				if (result == 'false') {
					var id = "#dis_email_id";
					var msg = "Email Already Exist.";
					err_msg(id, msg);
					return false;
				}
			},
			error: function(e) {
				sessionTimeout(e);
			}

		});
	}
	return true;
}
function keySearch(event) {
	//    ajaxStart();
	showLoader();
	var e = event || window.event;
	var ajaxTime = "";
	$(".closeSearchFeild").hide();
	$("#pageBodyContent").val('');
	var totalTime = "";
	var lastKey = event.key;
	var userval = $('#SearchResult').val();
	$("#searchedValue").val(userval);
	$("#downloadData").next().next().hide();
	//    if(userval.indexOf("_") != -1){
	var domainValue = $('#SelectedValue').val();
	var dxpAdavanceSearchOptions = $('#dxpAdavanceSearchOptions').val();
	if (dxpAdavanceSearchOptions != null && dxpAdavanceSearchOptions != undefined
		&& dxpAdavanceSearchOptions != '' && dxpAdavanceSearchOptions == 'D') {
		showLoader();
		dictionaryAutoComplete(domainValue);

	} else if (dxpAdavanceSearchOptions != null && dxpAdavanceSearchOptions != undefined
		&& dxpAdavanceSearchOptions != '' && dxpAdavanceSearchOptions == 'PR') {
		//        showLoader();
		repositoryAutoComplete(event);
		//        stopLoader();
	} else {

		userval = userval.trim();
		var responseObj = {};
		//    var userval = "SIZE";
		$(".searchMainWrap .has-search").css({ "width": "600" });
		$(".searchMainWrap").css({ "margin-left": "30px", "margin-top": "inherit" });
		$(".searchbutton").removeClass("searchbutton");
		$("#rightsearchicon").addClass("replacedSearchButton");
		$(".searchResultsDiv").show();
		$(".backbutton").show();
		$(".selectDropDown").show();
		$(".visualizationDashboardView").hide();
		$("#filterDownArrowIconID").show();
		$('.ui-autocomplete').html('');
		//        $("#dxpAdavanceSearchClass").hide();


		//var
		if (userval != null && userval != '') {
			$(".clear_input").show();
		} else {
			$(".clear_input").hide();
		}
		if (e.keyCode == 32 //Space
			|| e.keyCode == 45 //Insert
			|| e.keyCode == 33 //Page Up
			|| e.keyCode == 34 //Page Down
			|| e.keyCode == 36//Home
			|| e.keyCode == 16 //Shift
			|| e.keyCode == 17 //Ctrl
			|| e.keyCode == 18 //Alt
			|| e.keyCode == 35//End
			|| e.keyCode == 37 //Left arrow
			|| e.keyCode == 38 //Up arrow
			|| e.keyCode == 39 //Right arrow
			|| e.keyCode == 40//Down arrow
		) {
			console.log('Ajax Not sent');
		} else {
			if (e.keyCode == 13) { //Enter
				showLoader();
				$("#firstDxpSplitter").show();
				$("#filterDownArrowIconID").find('img').hide();
				firstPanelShowFlag = true;
				//                getFirstPanelShow(event);
				$("#SearchResult").val(userval);
				if (userval != null && userval != '' && userval != undefined) {
					ajaxStart();
					$("#typedResult").val(userval);
					showLoader();
					searchLoadETL();
					//                    smartTextSearch(userval, domainValue, 'Y');
					stopLoader();

				} else {
					var modalObj = {
						title: 'Message',
						body: "Enter a keyword of at least 1 char,ignoring special chars(@.,;:/ etc)  to search"
					};
					createModal("dataDxpSplitterValue", modalObj);
				}

			} else {
				userval = userval.replace(/\s\s+/g, ' ');
				$("#result").val(userval);
				if (userval.length == 0) {
					//                clearTextSearch();
					//                    $(".searchResultsDiv").hide();

				} else {
					//                        
					console.log('if condition::::sending ' + userval + " to server....");
					ajaxTime = new Date().getTime();
					$("#intellisensebox").attr("data-space", "yes");
					showLoader();
					var locale_dd = $("#localedd").find(':selected').data('code');
					var languageid = ""
					try {
						languageid = $("#localedd").find(':selected').val();
						languageid = languageid.replace(/#/g, '_');
					} catch (e) {
					}
					$.ajax({
						type: "POST",
						url: "searchSuggestion",
						data: {
							'searchtext': userval,
							'domainValue': domainValue,
							'locale': 'en_US',
							'langID': '1007-1_LG-000001_1',
							'fuzzyFlag': 'true',
							'advValue': dxpAdavanceSearchOptions,
						},
						success: function(response) {
							ajaxStop();
							stopLoader();
							firstPanelShowFlag = true;
							secondPanelShowFlag = true;
							thirdPanelShowFlag = true;
							$("#filterDownArrowIconID").hide();
							$("#searchDxpSplitter").hide();
							if (response != null && response != '') {
								$("#intellisense").html("");
								responseObj = JSON.parse(response);
								if (responseObj['flag']) {
									//suggestion
									$("#intellisense").html(responseObj['suggestion']);
									totalTime = new Date().getTime() - ajaxTime;
									totalTime = parseInt(totalTime) / 1000;
									$("#text_count").text("(Showing " + responseObj['currentRecords'].toLocaleString() + " of " + responseObj['totalRecords'].toLocaleString() + " in " + totalTime + " Sec)");
									$("#intellisensebox").show();
								} else {
									ajaxStop();
									showLoader();
									if (responseObj['totalRecords'] == '0') {
										//                                    $("#text_count").text("No record(s) found");
										//                                    $("#tooltipdiv").html("");
										//                                    $("#tooltipdiv").jqxTooltip({'content': 'No record(s) found', theme: 'energyblue'});
										//                                    $("#tooltipdiv").jqxTooltip("open");
										$("#intellisense").html("<div  class='ac-items' id='updateIntellisense_0'>No record's found</div>");
										$("#intellisensebox").hide();
									} else {
										ajaxStop();
										//                                    $("#tooltipdiv").html("");
										//                                    $("#tooltipdiv").jqxTooltip({content: 'Enter a keyword of at least 3 chars,ignoring special chars(@.,;:/ etc)  to search', theme: 'energyblue'});
										//                                    $("#tooltipdiv").jqxTooltip("close");
										$("#intellisense").html("<div  class='ac-items' id='updateIntellisense_0'>No record's found</div>");
										$("#intellisensebox").hide();
									}

								}
							} else {
								ajaxStop();
								showLoader();
								//                            $("#text_count").text("No record(s) found");
								//                            $("#tooltipdiv").html("");
								//                            $("#tooltipdiv").jqxTooltip({'content': 'No record(s) found', theme: 'energyblue'});
								//                            $("#tooltipdiv").jqxTooltip("open");
								$("#intellisense").html("<div  class='ac-items' id='updateIntellisense_0'>No record's found</div>");
								$("#intellisensebox").hide();
							}
							ajaxStop();
						},
						error: function(e) {
							console.log(e);
							ajaxStop();
							showLoader();
							sessionTimeout(e);
						}

					});

				}


			}
			stopLoader();
		}
		//        
	}
	//    stopLoader();
}
function updateIntellisense(row) {
	$("#typedResult").val($("#result").val());
	console.log("updateIntellisense:::" + row);
	var selectedStr = $("#updateIntellisense_" + row).text();
	console.log("selectedStr:::" + selectedStr);
	var replacestring = selectedStr.replace(/<b>|<\/b>/g, "");
	replacestring = replacestring.replace(/<b class="fuzzyItem">|<\/b>/g, "");
	replacestring = replacestring.replace(/<b class='fuzzyItem'>|<\/b>/g, "");
	$("#result").val(replacestring);
	var paramArray = [];
	var modalObj = {
		title: 'Message',
		body: replacestring
	};
	createModal("dataDxpSplitterValue", modalObj);
	//    searchResults('S', 'resultSuggestion', paramArray);

}
function clearTextSearch() {
	$(".innerSearchDiv").hide();
	$(".closeSearchFeild").hide();
	$(".settingsIconList").hide();
	$("#searchFeildToggleIcon").show();
	$(".searchFeildIconandText").show();
	$("#rightsearchicon").addClass("searchbutton");
	$("#rightsearchicon").removeClass("replacedSearchButton");
	$(".searchResultsDiv").hide();
	$(".backbutton").hide();
	$("#SearchResult").val('');
	$(".searchResultsDiv").hide();
	$(".selectDropDown").hide();
	$("#filterDownArrowIconID").hide();
	$(".searchMainWrap .has-search").css({ "width": "400" });
}
function fetchTabData(tabId, tabOldId, dependentAccorId, currntAccorId, wrapInd) {
	if (tabId != null && tabId.indexOf("_OLD") == -1) {
		globalTabId = tabId;
	}
	alert(tabId);
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	ssDatePickerObj = {};
	try {
		ssDatePickerObj = JSON.parse($("#ssDatePickerObj").val());
	} catch (e) {
	}
	$.datepicker.regional['datpicker'] = ssDatePickerObj;
	$.datepicker.setDefaults($.datepicker.regional['datpicker']);
	var fetchTabdata = $("#" + tabId + "_HIDDEN").attr('data-fetchInd') == "false" ? false : true;
	var dependentAccorId = dependentAccorId;
	if (currntAccorId > -1) {
		ajaxStart();
		$("[class*=_OLD]").addClass("ui-state-disabled");
		$(".ui-state-disabled").not(dependentAccorId).next("div").hide();
		$(dependentAccorId).next("div").toggle();
		$(".visionAccordionSeperator").remove();
		$("#" + tabId + "_TABLE").after("<div class='visionAccordionSeperator'></div>");
		fetchTabData(tabOldId, tabId, '', '-1', 1);
	}
	if (true) {   //var progress_count = 3;
		alert("true");
		$("#" + tabId).attr('data-fetchInd', true);
		var jsonOBJ = {};
		var basicData = {};
		jsonOBJ.feildIds = [];
		jsonOBJ.feildValues = [];
		$("#" + tabId + "_TABLE :input").each(function() {
			// alert("tabId:::"+tabId);
			var textid = $(this).attr("id");
			var textval = "";
			console.log("textid::::" + textid);
			if ($("#" + textid).val() !== null && $("#" + textid).val() !== "") {
				var type = $(this).attr("type");
				textval = $(this).val();
				if (type != 'hidden') {
					if (textval != null && textval != '') {
						textval = textval.toUpperCase();
					}
				}
			}
			//                  jsonOBJ.ids.push(textid.toLowerCase());
			jsonOBJ.feildIds.push(textid);
			jsonOBJ.feildValues.push(textval);

		});
		$("#mat_creation_form_table :input").each(function() {
			var textid = $(this).attr("id");
			var type = $(this).attr("type");
			var textval = $(this).val();
			if (type != 'hidden') {
				if (textval != null && textval != '') {
					textval = textval.toUpperCase();
				}
			}
			//                  jsonOBJ.ids.push(textid.toLowerCase());
			if (textid != null && textid != 'CREATE_DATE') {
				basicData[textid] = textval;
			}

			if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
				var columnNames = $("#" + textid).val();
				var columnsArray = columnNames.split(",");

				var hiddenIds = textid.split("HIDDEN_");
				var hiddenVal = $("#" + hiddenIds[1]).val();
				for (var i = 0; i < columnsArray.length; i++) {
					basicData[columnsArray[i]] = hiddenVal;
				}

			}
		});
		jsonOBJ.basicData = basicData;
		// alert(":::::::::"+JSON.stringify(jsonOBJ));
		// fetchTabdata = true;           
		var urlName = tabId.replace(/\_/g, "");
		var url = "selectRecord";
		console.log("JSON.stringify(jsonOBJ)::" + tabId + "::" + JSON.stringify(jsonOBJ));
		ajaxStart();
		$("body").css("pointer-events", "none");
		$.ajax({
			url: url,
			type: "post",
			traditional: true,
			dataType: 'html',
			//                    dataType: "json",
			// contentType: "text/html;charset=utf-8",
			cache: false,
			data: {
				jsonData: JSON.stringify(jsonOBJ),
				gridId: tabId,
				panelId: $("#panelId").val()
			},
			success: function(response) {
				ajaxStop();//20
				if (response != null && response != '' && response != undefined && response.indexOf("failed") > -1) {
					$("#selectedGridObj").val(response);
					var responseObj = JSON.parse(response);
					var formDefaultValues = responseObj['defaultValues'];
					var wthCount = responseObj['ssAdaniWthCount'];
					var gridinitparamobj = {};
					var gridCaseSensitiveCols = "";
					gridinitparamobj = responseObj['gridInitParamObj'];
					if (gridinitparamobj != null && gridinitparamobj != 'undefined' && gridinitparamobj != '' && gridinitparamobj != 'null') {
						gridCaseSensitiveCols = gridinitparamobj['uuu_GridCaseSensitiveCols'];
						if (gridCaseSensitiveCols != null &&
							gridCaseSensitiveCols != 'undefined' &&
							gridCaseSensitiveCols != 'null' &&
							gridCaseSensitiveCols == 'PROPERTY_UOM' &&
							gridCaseSensitiveCols != '') {
							$("#charOrpicMMLowercase").val("PROPERTY_UOM");
						} else {
							$("#charOrpicMMLowercase").val("N");
						}
					}


					$("#defaultValues").val(formDefaultValues);
					$("#" + tabId + "_defaultValues").remove();
					$("#mat_creation_form_table").append("<input type='hidden' id='" + tabId + "_defaultValues' />");


					$("body").css("pointer-events", "auto");
					ajaxStop();//21
					if (response.indexOf("Failed to Fetch Record!") > -1) {
						$("#" + tabId + "_Update").attr("data-view", "FORM-VIEW");
						$("#" + tabId + "_Delete").attr("data-view", "FORM-VIEW");

						$("#" + tabId + '_TABLE').show();
						$("#" + tabId).hide();

						//CLEARING GRID ROWS
						newremoveAllGridRows(tabId);

						//FORM CRUID OPS ICONS DISPLAY
						genericFormViewIconsDisp(tabId, 0);

						tabOperation(tabId, "add");
						$("#" + tabId + "_Grid_View").hide();
						$("#" + tabId).hide();
						$("#" + tabId + "_Update").show();
						psCount(tabId);
					} else {
						//                            alert("response: in else" + response);
						var jsnobj = JSON.parse(response);
						var noOfRecords = jsnobj.lengthPay;
						var erpData = jsnobj.erpData;
						var dataView = jsnobj['view'];
						if (dataView == 'FORM-VIEW')
						//                            if (noOfRecords == 1)

						{
							alert("noOfRecords::" + noOfRecords);
							// tabOperation('MM_DOCUMENTS_CHNG','refresh')
							$("#" + tabId + "_Update").attr("data-view", "FORM-VIEW");
							$("#" + tabId + "_Delete").attr("data-view", "FORM-VIEW");
							$("#" + tabId + "_Delete").show();
							$("#" + tabId + "_Update").show();
							$("#" + tabId + '_TABLE').show();
							$("#" + tabId + '_ICON').css("display", "block");
							//console.log("tabId::::"+tabId);
							$("#" + tabId).hide();
							//CLEARING GRID ROWS
							newremoveAllGridRows(tabId);
							//GRID CRUID OPS ICONS DISPLAY
							genericFormViewIconsDisp(tabId, jsnobj.lengthPay);
							//WITHHOLD TAN ENABLING AND DISABLING FEILDS BASED ON Lower TDS Applicability 
							disableOrEnableWthTanTabAttribuetes();
							//    alert(erpData);
							if (erpData != null && erpData == 'Y') {
								var erpDataObj = jsnobj;
								erpTab(tabId, erpDataObj, erpData, dataView);

							} else {
								//alert(JSON.stringify(jsnobj.data));
								$("#" + tabId + '_TABLE').html(jsnobj.data);
							}

							if (erpData != null && erpData == 'Y') {
								$("#" + tabId).show();
							} else {
								$("#" + tabId).hide();
							}
							if ($("body").height() <= $(window).height()) {
								$("#bottom_arrow").hide();
								$("#top_arrow").hide();
							}
							if ($("#O_1IEXCD1").val() == "NA") {
								$("#O_1IEXCD2").val("");
								//                                    $("#O_1IEXCD2").attr("disabled", "disabled");
								$("#O_1IEXCD2").attr("readonly", true);
							}
							if ($("#O_1ISERN1").val() == "NA") {
								$("#O_1ISERN2").val("");
								//                                    $("#O_1ISERN2").attr("disabled", "disabled");
								$("#O_1ISERN2").attr("readonly", true);
							}

							var reciepientType = "OT";
							var panCharTop;
							panCharTop = $("#PAN_NUMBER").val();
							if (panCharTop && panCharTop.charAt(3) == "C") {
								reciepientType = "CO";
							}
							$("#QSREC").val(reciepientType);

							if ($("#LAND1").val() != 'IN') {
								// for other then India
								$("#PSTLZ").attr('maxlength', '15');
								$("#PSTLZ").attr("data-regex", "^[a-zA-Z0-9 -]+$");

								$("#ORT01").attr('readonly', true);
								$("#ddORT01").show();
								$("#ORT02").attr('readonly', true);
								$("#ddORT02").show();
								$("#O_1IPANNO").attr('data-mandatory', "O");
								$('.lblMandO_1IPANNO').hide();
								$("#STCD3").attr('data-mandatory', "O");
								$('.lblMandSTCD3').hide();
								$("#GST_NUMBER").attr('data-mandatory', "O");
								$('.lblMandGST_NUMBER').hide();
								$("#VEN_CLASS").attr('data-mandatory', "O");
								$('.lblMandVEN_CLASS').hide();
							} else {

								$("#PSTLZ").attr('maxlength', '6');
								$("#PSTLZ").attr("data-regex", "^[1-9]{1}[0-9]{5}$");
								$("#ORT01").attr('readonly', true);
								$("#ddORT01").show();
								$("#ORT02").attr('readonly', true);
								$("#ddORT02").show();
							}

							if (($("#PLANT").val() == "500" || $("#PLANT").val() == "700") &&
								($("#ACCOUNT_GROUP").val() == "AWFR") &&
								($("#COMPANY_CDE").val() == "1111")) {
								$("#SORT2").attr('readonly', true);
								$("#ddSORT2").show();
							} else {
								$("#SORT2").attr('readonly', false);
								$("#ddSORT2").hide();
							}
							psCount(tabId);
							alert("form::" + $("#" + tabId + '_Delete').css("display"));
							if (tabId.indexOf('WTH_TAN_DATA') > -1) {
								if (wthCount == 0) {
									$("#" + tabId + '_ICON').css("display", "none");
								} else {
									$("#" + tabId + '_ICON').css("display", "block");

								}
							}
						} else if (gridinitparamobj != null
							&& !jQuery.isEmptyObject(gridinitparamobj)
							&& gridinitparamobj['tableview'] == 'Y') {

							$("#" + tabId + "_Update").attr("data-view", "TABLE-VIEW");
							$("#" + tabId + "_Delete").attr("data-view", "TABLE-VIEW");
							$("#" + tabId + "_Insert").attr("data-view", "TABLE-VIEW");

							$("#" + tabId + "_TABLE").hide();
							//$("#tg-wrap").remove();
							if (jsnobj.gridEditable) {
								$("#tg-wrap").remove();
								$("#tg-wrap1").removeClass('visionCharacteristicsTbl');
								$("#tg-wrap1").addClass('visionCharacteristicsTbl');
							} else {
								$("#tg-wrap1").remove();
							}

							$("#" + tabId + "tbl").remove();
							var columns = jsnobj.columns;
							var datafields = jsnobj.datafields;
							var localdata = jsnobj.data;
							initialTblViewData = "";
							initialTblViewData = localdata;
							initialTblViewCols = jsnobj.columns;
							$("#" + tabId + "_Update").attr('data-localdata', JSON.stringify(localdata));
							$("#" + tabId + "_Update").attr('data-datafields', JSON.stringify(jsnobj.columns));
							//                                console.log("tableViewStr::::"+jsnobj['tableViewStr']);
							var columns = jsnobj.columns;

							var gridPropObj = jsnobj.gridPropObj;
							$("#" + tabId).html(jsnobj['tableViewStr']);
							var charOrpicMMLowercase = $("#charOrpicMMLowercase").val();
							if (charOrpicMMLowercase != null && charOrpicMMLowercase != "undefined"
								&& charOrpicMMLowercase != "" && charOrpicMMLowercase == "PROPERTY_UOM") {
								for (var i = 0; i < responseObj['data'].length; i++) {
									$("#tb" + charOrpicMMLowercase + i).css("text-transform", "none");
								}
							}
							$("#" + tabId + "tbl").each(function() {

								if ($(this).find('thead').length > 0 && $(this).find('th').length > 0) {
									// Clone <thead>
									var $w = $(window),
										$t = $(this),
										$thead = $t.find('thead').clone(),
										$col = $t.find('thead, tbody').clone();
									$t.addClass('sticky-enabled')
										.css({

											margin: 0,

											width: '100%'

										}).wrap('<div class="sticky-enabled" />');
									$('.sticky-wrap tbody').addClass('sticky-header');
									if ($t.hasClass('overflow-y'))
										$t.removeClass('overflow-y').parent().addClass('overflow-y');
									// Create new sticky table head (basic)
									$t.after('<table class="sticky-thead" />');
									// If <tbody> contains <th>, then we create sticky column and intersect (advanced)
									if ($t.find('tbody th').length > 0) {
										$t.after('<table class="sticky-col" /><table class="sticky-intersect" />');
									}

									// Create shorthand for things
									var $stickyHead = $(this).siblings('.sticky-thead'),
										$stickyCol = $(this).siblings('.sticky-col'),
										$stickyInsct = $(this).siblings('.sticky-intersect'),
										$stickyWrap = $(this).parent('.sticky-wrap');
									$stickyHead.append($thead);
									$stickyCol
										.append($col)
										.find('thead th:gt(0)').remove()
										.end()
										.find('tbody td').remove();
									$stickyInsct.html('<thead><tr><th>' + $t.find('thead th:first-child').html() + '</th></tr></thead>');
									// Set widths
									var setWidths = function() {
										$t
											.find('thead th').each(function(i) {
												$stickyHead.find('th').eq(i).width($(this).width());
											})
											.end()
											.find('tr').each(function(i) {
												$stickyCol.find('tr').eq(i).height($(this).height());
											});
										// Set width of sticky table head
										$stickyHead.width("100%");
										//                                        console.log($t.width());                                       
										// Set width of sticky table col
										$stickyCol.find('th').add($stickyInsct.find('th')).width($t.find('thead th').width())
									},
										repositionStickyHead = function() {
											// Return value of calculated allowance
											var allowance = calcAllowance();
											// Check if wrapper parent is overflowing along the y-axis
											if ($t.height() > $stickyWrap.height()) {
												// If it is overflowing (advanced layout)
												// Position sticky header based on wrapper scrollTop()
												if ($stickyWrap.scrollTop() > 0) {
													// When top of wrapping parent is out of view
													$stickyHead.add($stickyInsct).css({
														opacity: 1,
														top: $stickyWrap.scrollTop() - 1
													});
													$(window).resize(function() {
														if ($(window).width() <= 500) {
															$(".visionHeaderMain").css("position", "absolute");
														}
													}).resize();
												} else {
													// When top of wrapping parent is in view
													$stickyHead.add($stickyInsct).css({
														opacity: 0,
														top: 0
													});
													$(".visionHeaderMain").css("position", "fixed");
												}
											} else {
												// If it is not overflowing (basic layout)
												// Position sticky header based on viewport scrollTop
												if ($w.scrollTop() > $t.offset().top && $w.scrollTop() < $t.offset().top + $t.outerHeight() - allowance) {
													// When top of viewport is in the table itself
													$stickyHead.add($stickyInsct).css({
														opacity: 1,
														top: $w.scrollTop() - $t.offset().top - 1
													});
													$(window).resize(function() {
														if ($(window).width() <= 500) {
															$(".visionHeaderMain").css("position", "absolute");
														}
													}).resize();
												} else {
													// When top of viewport is above or below table
													$stickyHead.add($stickyInsct).css({
														opacity: 0,
														top: 0
													});
													$(".visionHeaderMain").css("position", "fixed");
												}
											}
										},
										repositionStickyCol = function() {
											if ($stickyWrap.scrollLeft() > 0) {
												// When left of wrapping parent is out of view
												$stickyCol.add($stickyInsct).css({
													opacity: 1,
													left: $stickyWrap.scrollLeft()
												});
											} else {
												// When left of wrapping parent is in view
												$stickyCol
													.css({ opacity: 0 })
													.add($stickyInsct).css({ left: 0 });
											}
										},
										calcAllowance = function() {
											var a = 0;
											// Calculate allowance
											$t.find('tbody tr:lt(3)').each(function() {
												a += $(this).height();
											});
											// Set fail safe limit (last three row might be too tall)
											// Set arbitrary limit at 0.25 of viewport height, or you can use an arbitrary pixel value
											if (a > $w.height() * 0.25) {
												a = $w.height() * 0.25;
											}

											// Add the height of sticky header
											a += $stickyHead.height();
											return a;
										};
									setWidths();
									$t.parent('.sticky-wrap').scroll($.throttle(250, function() {
										repositionStickyHead();
										repositionStickyCol();
									}));
									stopLoader();
									$w.load(setWidths)
										.resize($.debounce(250, function() {
											setWidths();
											repositionStickyHead();
											repositionStickyCol();
										}))
										.scroll($.throttle(250, repositionStickyHead));

								}
							});

							//                                $(".visionRegisterMaterialAccordians .sticky-wrap")
							//                                        .css("height", gridPropObj.height);

							if (!jsnobj.gridEditable
								&&
								$("#REQ_NUMBER").length > 0
								&&
								$("#baskettypehid").val().toUpperCase().indexOf('CHANGE') > -1
							) {
								try {
									var position = $('#MM_PROPERTIES_CHNG').position();
									$("#tg-wrap1").removeClass('visionCharacteristicsTbl');
									console.log('position:' + position);
									//   $("#tg-wrap1").css('position', 'absolute');
									$("#tg-wrap1").css('top', position.top);
									$("#tg-wrap1").css('right', '1.2em');
								} catch (e) {

								}
							}
						} else {
							erpTab(tabId, jsnobj, jsnobj['erpData'], 'GRID-VIEW');
							alert("dataView:::" + dataView);
							//  $("#" + tabId).jqxGrid({autoheight: true});
							$('#' + tabId + '_Add').click(function() {

								$("#" + tabId).css("display", "none");
								$("#" + tabId).css("display", "none");
							});
						}//grid end
					}
					$("#" + tabId + "_defaultValues").val(formDefaultValues);
					$("#" + tabId + "_TABLE" + " :input[data-type='D']").each(function() {
						var id = $(this).attr('id');
						var isEditable = $("#" + id).attr('data-editable');
						if (isEditable == "Y") {
							$("#" + id).datepicker({
								changeMonth: true,
								changeYear: true,
								dateFormat: "dd-mm-yy",
								showOn: "button",
								buttonImage: 'images/date_picker_icon.png',
								buttonImageOnly: true
							});
						}
					});
					$("#" + tabId + "_FORM" + " :input[data-type='D']").each(function() {
						var id = $(this).attr('id');
						var isEditable = $("#" + id).attr('data-editable');
						if (isEditable == "Y") {
							$("#" + id).datepicker({
								changeMonth: true,
								changeYear: true,
								dateFormat: "dd-mm-yy",
								showOn: "button",
								buttonImage: 'images/date_picker_icon.png',
								buttonImageOnly: true
							});
						}
					});
					var tabOldObj = {};

					$("#" + tabId + "_TABLE" + " :input").each(function() {
						var textid = $(this).attr("id");
						var type = $(this).attr("type");
						var textval = $(this).val();
						if (type != 'hidden') {
							if (textval != null && textval != '') {
								textval = textval.toUpperCase();
							}
						}
						if (type != null && type == 'checkbox') {//
							if ($("#" + textid).is(':checked')) {
								textval = "Y";
							} else {
								textval = "N";
							}
						}
						//                  jsonOBJ.ids.push(textid.toLowerCase());
						if (textid != null && textid != 'CREATE_DATE') {
							tabOldObj[textid] = textval;
						}
					});
					if (tabOldObj != null) {
						tabsOldData[tabId] = tabOldObj;
					}
					/*
					 table View changes Data 
					 **/
					// code for getting table-view old data
					var tabchangesOldObject = {};
					$("[id*=" + tabId + "] :input").each(function() {
						var textid = $(this).attr("id");
						var type = $(this).attr("type");
						var textval = $(this).val();
						if (type != 'hidden') {
							if (textval != null && textval != '') {
								textval = textval.toUpperCase();
							}
						}
						if (type != null && type == 'checkbox') {//
							if ($("#" + textid).is(':checked')) {
								textval = "Y";
							} else {
								textval = "N";
							}
						}
						//                  jsonOBJ.ids.push(textid.toLowerCase());
						if (textid != null && textid != 'CREATE_DATE') {
							tabchangesOldObject[textid] = textval;
						}
					});

					if (tabchangesOldObject != null) {
						tabsOldData[tabId + "_tbl"] = tabchangesOldObject;
					}

					/**/
					if (!wrapInd) {
						$("#" + tabId + "_TABLE" + " select").each(function() {
							var id = $(this).attr('id');
							var onChange = $("#" + id).attr('onchange');
							if (onChange != null && onChange != "undefined") {
								//                            eval(onChange);
							}
						});
					}
					try {
						$("#" + tabId).jqxGrid('updatebounddata');
					} catch (e) {
					}
					try {
						$("#" + tabId).jqxGrid("clearselection");
					} catch (e) {
					}
					$("#" + tabId + "_Update").show();
					$("#" + tabId + "_Delete").show();
					alert("global::" + $("#" + tabId + '_Delete').css("display"));
					if (wrapInd) {

						$("#" + tabId + "_ICON").hide();
						if (tabId.indexOf('PROPERTIES_OLD') > -1) {
							$("#" + tabId).addClass("visionOldChar");
							$(".visionAccordionSeperator").css("display", "none");

						}
						/* To Swapping OLD-NEW Data : Start*/
						ajaxStop();
					}
				}
			},
			error: function(e) {
				$("body").css("pointer-events", "auto");
				ajaxStop();//22
				sessionTimeout(e);
			}
		});
	}
	executed = false;
	setTimeout(changeflagFuction, 300);
}
function newremoveAllGridRows(tabId) {
	$("#" + tabId).jqxGrid('clear');
}
function genericFormViewIconsDisp(tabId, rowsCount) {
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	if (rowsCount == 0) {
		$("#" + tabId + '_Grid_View').hide();
		$("#" + tabId + '_Update').show();
		$("#" + tabId + '_Delete').hide();
		$("#" + tabId + '_Add').hide();
	} else if (rowsCount == 1) {
		alert("else if (rowsCount == 1)");
		$("#" + tabId + '_Grid_View').hide();
		$("#" + tabId + '_Update').show();
		$("#" + tabId + '_Delete').show();
		$("#" + tabId + '_Add').show();
	} else if (rowsCount > 1) {
		$("#" + tabId + '_Grid_View').hide();
		$("#" + tabId + '_Update').hide();
		$("#" + tabId + '_Delete').hide();
		$("#" + tabId + '_Add').show();
	}
	alert("genericFormViewIconsDisp::" + $("#" + tabId + '_Delete').css("display"));
}
function tabOperation(tableName, operation) {
	if (operation == 'download') {
		//        downloadCodeOfConduct(tableName)
	} else if (operation == 'SOWUpdate') {
		//        updateScopeOfWork(tableName);
	} else if (operation == 'SOWPreview') {
		//        previewScopeOfWork(tableName);
	} else if (operation == 'fillDown') {
		//        populateTabGridFillDownData(tableName);
	} else if (operation != null && operation == 'LapsTimeReport') {
		//        getLapsTimeReport(tableName);
	} else {
		labelObject = {};
		try {
			labelObject = JSON.parse($("#labelObjectHidden").val());
		} catch (e) {

		}
		var dataView = $("#" + tableName + "_Update").attr("data-view");

		alert("operation:::" + operation + ":::dataView::::" + dataView);
		if (dataView == null) {
			try {
				var sourceex = $('#' + tableName).jqxGrid('source');
				if (sourceex != null) {
					dataView = "GRID-VIEW";
				} else {
					dataView = "FORM-VIEW";
				}
			} catch (e) {
			}
		}
		alert("operation::A:" + operation + ":::dataView::::" + dataView);
		var basicData = {};
		var basicDataAudit = {};
		$("#mat_creation_form_table :input").each(function() {
			var textid = $(this).attr("id");
			var type = $(this).attr("type");
			var textval = $(this).val();
			if (type != 'hidden') {
				if (textval != null && textval != '') {
					textval = textval.toUpperCase();
				}
			}
			if (textid != null && textid != 'CREATE_DATE') {

				basicData[textid] = textval;
			}
			if (textid != null) {

				basicDataAudit[textid] = textval;
			}

			if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
				var columnNames = $("#" + textid).val();
				var columnsArray = columnNames.split(",");

				var hiddenIds = textid.split("HIDDEN_");
				var hiddenVal = $("#" + hiddenIds[1]).val();
				for (var i = 0; i < columnsArray.length; i++) {
					if (hiddenVal != null) {
						hiddenVal = hiddenVal.toUpperCase();
					}
					basicData[columnsArray[i]] = hiddenVal;
					basicDataAudit[columnsArray[i]] = hiddenVal;
				}
			}
		});
		if (operation == "update" || operation == 'checkingTabData') {
			// ajaxStart();//1
			var lasteditedfield = $('#' + tableName).attr('data-last-ed-field');
			var lasteditedrow = $('#' + tableName).attr('data-last-ed-row');
			try {
				$('#' + tableName).jqxGrid('endcelledit', lasteditedrow, lasteditedfield, false);
			} catch (e) {
			}
			hideErrors();
			var errorCount = 0;
			if (dataView == "FORM-VIEW") {
				errorCount = 0;
				var v_ag = $("#hiddenAccountGroup").val();

				if (v_ag != null && (v_ag == "Material & Service (Foreign)")) {
					$("#BANKL").attr("data-mandatory", "O");
					$("#BANKL").prop("readonly", true);
				}
				var jsonOBJ = {};
				var erpDataGridId = $("#erpDataGridId").val();
				var selectedTabOldData = tabsOldData[tableName];
				$("table#" + tableName + "_TABLE :input").each(function() {
					var id = $(this).attr('id');
					var mand = $(this).attr("data-mandatory");
					var label = $(this).attr("data-label");
					mand = (mand === "M") ? "M" : "O";
					if (label != null && label == "Bank Key(IFSC)" && (v_ag != null && v_ag == "Material & Service (Foreign)")) {
						$("#BANKL").attr("data-regex", "");
					}
					var regex = $(this).attr("data-regex");
					var returnBoolean = regexFunction(id, regex, mand, tableName, label);
					if (returnBoolean == false) {
						errorCount++;
						return false;
					}
				});
				console.log("errorCount:::" + errorCount);
				if (errorCount == 0) {


					jsonOBJ.feildIds = [];
					jsonOBJ.feildValues = [];
					console.log(tableName + ":::textid:::");
					var matchedCount = 0;
					var gridIdHiddenValue = "UPDATE";
					$("table#" + tableName + "_TABLE :input").each(function() {
						var textid = $(this).attr("id");
						var type = $(this).attr("type");
						var textval = $(this).val();
						console.log("textid:::" + textid);
						if (type != 'hidden') {
							if (textval != null && textval != '') {
								textval = textval.toUpperCase();
							}
						}

						// var type = $(this).attr("type");
						jsonOBJ.feildIds.push(textid);
						if (type != null && type == 'checkbox') {//
							if ($("#" + textid).is(':checked')) {
								textval = "Y";
							} else {
								textval = "N";
							}
						}
						jsonOBJ.feildValues.push(textval);
						if (textid != null && textid.indexOf("AUDIT_ID") > -1) {
							basicData[textid] = textval;
						}
						var textOldVal = "";
						if (selectedTabOldData != null) {
							textOldVal = selectedTabOldData[textid];

						}
						console.log(textval + ":::" + textid + "::" + textOldVal);
						if (textval != textOldVal) {
							matchedCount++;
						}
						var tableNameHidden = tableName + "_HIDDEN";
						if (textid == tableNameHidden) {
							gridIdHiddenValue = $("#" + textid).val();
						}
					});
					console.log("jsonOBJ:::" + JSON.stringify(jsonOBJ));

					if (gridIdHiddenValue == 'INSERT' && matchedCount == 0) {
						matchedCount = 1;
					}
					if (matchedCount > 0 || operation == 'checkingTabData') {
						jsonOBJ.basicData = basicData;
						console.log("jsonOBJ.feildIds:::" + JSON.stringify(jsonOBJ.feildIds));
						console.log("jsonOBJ.feildValues:::" + JSON.stringify(jsonOBJ.feildValues));
						var jsonArray = [];
						jsonArray.push(jsonOBJ);
						UpdateOrDelete(JSON.stringify(jsonArray), dataView, tableName, operation);
					} else {
						ajaxStop();//8
						var results = "No Changes to Save";
						results = (labelObject[results] != null ? labelObject[results] : results);
						var dialogSplitMessage = dialogSplitIconText(results, "Y");
						$("#dialog").html(dialogSplitMessage);
						$("#dialog").dialog({
							title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
							modal: true,
							height: 'auto',
							minHeight: 'auto',
							minWidth: 300,
							maxWidth: 'auto',
							fluid: true,
							buttons: [{
								text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
								click: function() {
									$(this).html("");
									$(this).dialog("close");
									$(this).dialog("destroy");
									//   fetchTabData(tableName);
									try {
									} catch (e) {
									}
								}
							}],
							open: function() {
								$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
								$(".visionHeaderMain").css("z-index", "999");
								$(".visionFooterMain").css("z-index", "999");
							},
							beforeClose: function(event, ui) {
								$(".visionHeaderMain").css("z-index", "99999");
								$(".visionFooterMain").css("z-index", "99999");
							}
						});
					}
				}
			} else if (dataView == "TABLE-VIEW") {
				selectedDataArray = gridOperation(operation, tableName);
				if (Array.isArray(selectedDataArray) && selectedDataArray.length == 0) {
					ajaxStop();//9
					var results = "No Changes to Save";
					results = (labelObject[results] != null ? labelObject[results] : results);
					var dialogSplitMessage = dialogSplitIconText(results, "Y");
					$("#dialog").html(dialogSplitMessage);
					$("#dialog").dialog({
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						modal: true,
						height: 'auto',
						minHeight: 'auto',
						minWidth: 300,
						maxWidth: 'auto',
						fluid: true,
						buttons: [{
							text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
							click: function() {
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
								fetchTabData(tableName);
								$(tableName).jqxGrid('clearselection');
							}
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});

				} else if (!(Array.isArray(selectedDataArray))
					&& selectedDataArray.errorMesssage != null
					&& selectedDataArray.errorMesssage != '') {
					var errorMessageTable = "<table style='width: 100%;' border='1'>"
						+ "<tr><th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Property Name</th>"
						+ "<th style='background: #0071c5 none repeat scroll 0 0;color: #FFF;text-align: center'>Error Message</th>";
					errorMessageTable += selectedDataArray.errorMesssage;
					errorMessageTable += '</table>';

					labelObject = {};
					try {
						labelObject = JSON.parse($("#labelObjectHidden").val());
					} catch (e) {
					}
					console.log(errorMessageTable + "::::::::::::::::::");
					if (errorMessageTable !== "" && errorMessageTable !== null) {
						errorMessageTable = (labelObject[errorMessageTable] != null ? labelObject[errorMessageTable] : errorMessageTable);
						var dialogSplitMessage = dialogSplitIconText(errorMessageTable, "false");
						$("#dialog").html(errorMessageTable);
						$("#dialog").dialog({
							modal: true,
							title: (labelObject['Error'] != null ? labelObject['Error'] : 'Error'),
							textAlign: 'center',
							minWidth: 'auto',
							maxWidth: 'auto',
							height: 'auto',
							minHeight: 'auto',
							fluid: true,
							buttons: [{
								text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
								click: function() {
									//$(this).html("");
									$(this).dialog("close");
								}
							}],
							open: function() {
								$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
								$(".visionHeaderMain").css("z-index", "999");
								$(".visionFooterMain").css("z-index", "999");
							},
							beforeClose: function(event, ui) {
								$(".visionHeaderMain").css("z-index", "99999");
								$(".visionFooterMain").css("z-index", "99999");
							}
						});
					}
				} else {
					endoperation(selectedDataArray, tableName, dataView, operation, basicData);
				}
			} else if (dataView == "GRID-VIEW") {
				if (operation == 'checkingTabData') {
					selectedDataArray = $('#' + tableName).jqxGrid('getdisplayrows');
				} else {
					selectedDataArray = gridOperation(operation, tableName);
				}
				console.log("selectedDataArray::::" + selectedDataArray.length);
				console.log("selectedDataArray::758::" + JSON.stringify(selectedDataArray));
				alert(selectedDataArray.length);
				if (selectedDataArray == 0) {
					ajaxStop();//10
					var results = "No Changes to Save";
					results = (labelObject[results] != null ? labelObject[results] : results);
					var dialogSplitMessage = dialogSplitIconText(results, "Y");
					$("#dialog").html(dialogSplitMessage);
					$("#dialog").dialog({
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						modal: true,
						height: 'auto',
						minHeight: 'auto',
						minWidth: 300,
						maxWidth: 'auto',
						fluid: true,
						buttons: [{

							text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
							click: function() {
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
								// fetchTabData(tableName);
								$(tableName).jqxGrid('clearselection');
							}

						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});

				} else {
					endoperation(selectedDataArray, tableName, dataView, operation, basicData);
				}
			}//if 
		} else if (operation == "calculateStock") {
			// ajaxStart();//1
			//if 
			var lasteditedfield = $('#' + tableName).attr('data-last-ed-field');
			var lasteditedrow = $('#' + tableName).attr('data-last-ed-row');
			try {
				$('#' + tableName).jqxGrid('endcelledit', lasteditedrow, lasteditedfield, false);
			} catch (e) {
			}
			hideErrors();
			var errorCount = 0;
			if (dataView == "FORM-VIEW") {
				errorCount = 0;
				var v_ag = $("#hiddenAccountGroup").val();

				if (v_ag != null && (v_ag == "Material & Service (Foreign)")) {
					$("#BANKL").attr("data-mandatory", "O");
					$("#BANKL").prop("readonly", true);
				}
				var jsonOBJ = {};
				var erpDataGridId = $("#erpDataGridId").val();
				//            
				var selectedTabOldData = tabsOldData[tableName];
				$("table#" + tableName + "_TABLE :input").each(function() {
					var id = $(this).attr('id');
					var mand = $(this).attr("data-mandatory");
					var label = $(this).attr("data-label");
					mand = (mand === "M") ? "M" : "O";
					var regex = $(this).attr("data-regex");
					if (id != null && id != '' && id != 'undefined') {
						if (id == 'CRITICALITY' || id == 'SETSIZE' || id == 'INSTALLED_QUANTITY' || id == 'PREDICTABILITY' || id == 'MTBF') {
							var returnBoolean = regexFunction(id, regex, mand, tableName, label);
							if (returnBoolean == false) {
								errorCount++;
								return false;
							}
						}
					}
				});
				console.log("errorCount:::" + errorCount);
				if (errorCount == 0) {
					jsonOBJ.feildIds = [];
					jsonOBJ.feildValues = [];
					console.log(tableName + ":::textid:::");
					var matchedCount = 0;
					var gridIdHiddenValue = "UPDATE";
					$("table#" + tableName + "_TABLE :input").each(function() {
						var textid = $(this).attr("id");
						var type = $(this).attr("type");
						var textval = $(this).val();
						console.log("textid:::" + textid);
						if (type != 'hidden') {
							if (textval != null && textval != '') {
								textval = textval.toUpperCase();
							}
						}
						jsonOBJ.feildIds.push(textid);
						if (type != null && type == 'checkbox') {//
							if ($("#" + textid).is(':checked')) {
								textval = "Y";
							} else {
								textval = "N";
							}
						}
						jsonOBJ.feildValues.push(textval);
						if (textid != null && textid.indexOf("AUDIT_ID")) {
							basicData[textid] = textval;
						}
						var textOldVal = "";
						if (selectedTabOldData != null) {
							textOldVal = selectedTabOldData[textid];

						}
						console.log(textval + ":::" + textid + "::" + textOldVal);
						if (textval != textOldVal) {
							matchedCount++;
						}
						var tableNameHidden = tableName + "_HIDDEN";
						if (textid == tableNameHidden) {
							gridIdHiddenValue = $("#" + textid).val();
						}
					});
					console.log("jsonOBJ:::" + JSON.stringify(jsonOBJ));

					if (gridIdHiddenValue == 'INSERT' && matchedCount == 0) {
						matchedCount = 1;
					}
					jsonOBJ.basicData = basicData;
					console.log("jsonOBJ.feildIds:::" + JSON.stringify(jsonOBJ.feildIds));
					console.log("jsonOBJ.feildValues:::" + JSON.stringify(jsonOBJ.feildValues));
					var jsonArray = [];
					jsonArray.push(jsonOBJ);
					UpdateOrDelete(JSON.stringify(jsonArray), dataView, tableName, operation);
				}
			} else if (dataView == "TABLE-VIEW") {

				selectedDataArray = gridOperation(operation, tableName);
				alert(selectedDataArray.length);
				if (selectedDataArray.length == 0) {
					ajaxStop();//9
					var results = "No Changes to Save";
					results = (labelObject[results] != null ? labelObject[results] : results);
					var dialogSplitMessage = dialogSplitIconText(results, "Y");
					$("#dialog").html(dialogSplitMessage);
					$("#dialog").dialog({
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						modal: true,
						height: 'auto',
						minHeight: 'auto',
						minWidth: 300,
						maxWidth: 'auto',
						fluid: true,
						buttons: [{
							text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
							click: function() {
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
								fetchTabData(tableName);
								$(tableName).jqxGrid('clearselection');
							}
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});
				} else {
					endoperation(selectedDataArray, tableName, dataView, operation, basicData);
				}
			} else if (dataView == "GRID-VIEW") {
				selectedDataArray = gridOperation(operation, tableName);
				console.log("selectedDataArray::::" + selectedDataArray.length);
				console.log("selectedDataArray::758::" + JSON.stringify(selectedDataArray));
				alert(selectedDataArray.length);
				//console.log("selectedDataArray size:::::" + JSON.stringify(selectedDataArray));
				if (selectedDataArray == 0) {
					ajaxStop();//10
					var results = "No Changes to Save";
					results = (labelObject[results] != null ? labelObject[results] : results);
					var dialogSplitMessage = dialogSplitIconText(results, "Y");
					$("#dialog").html(dialogSplitMessage);
					$("#dialog").dialog({
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						modal: true,
						height: 'auto',
						minHeight: 'auto',
						minWidth: 300,
						maxWidth: 'auto',
						fluid: true,
						buttons: [{
							text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
							click: function() {
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
								// fetchTabData(tableName);
								$(tableName).jqxGrid('clearselection');
							}
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});

				} else {

					endoperation(selectedDataArray, tableName, dataView, operation, basicData);
				}
			}
		} else if (operation == "add") {
			var lasteditedfield = $('#' + tableName).attr('data-last-ed-field');
			var lasteditedrow = $('#' + tableName).attr('data-last-ed-row');
			try {
				$('#' + tableName).jqxGrid('endcelledit', lasteditedrow, lasteditedfield, false);
			} catch (e) {

			}
			$("table#" + tableName + "_Operators").show();
			$("table#" + tableName + "_TABLE :input").each(function() {
				$(this).val('');
				$(this).prop("checked", false);
			});
			$("table#" + tableName + "_TABLE select").each(function() {
				//            alert("select");
				$(this).prop('selectedIndex', 0);
				//            $(this).prop("checked", false);
				var id = $(this).attr('id');
				var onChange = $("#" + id).attr('onchange');
				if (onChange != null && onChange != "undefined") {
					eval(onChange);
				}
			});

			$("#" + tableName + "_HIDDEN").val("INSERT");
			//        $("#BKONT").val("01");

			if (dataView == "FORM-VIEW") {
				$("#" + tableName + "_TABLE").show();
				$("#" + tableName).show();
				//            $("#" + tableName).hide();
				dataView = "FORM-VIEW";
				$("#" + tableName + "_Update").attr("data-view", "FORM-VIEW");
				$("#" + tableName + "_Grid_View").show();
				$("#" + tableName + "_Delete").show();
				$("#" + tableName + "_Update").show();

				/* for setting default values*/
				var formDefaultValues = $("#" + tableName + "_defaultValues").val();
				if (formDefaultValues != null && formDefaultValues != '') {

				} else {
					formDefaultValues = $("#defaultValues").val();
				}
				//            alert("formDefaultValues:::form::" + formDefaultValues);
				var currentValue;
				var currentColumn;
				if (formDefaultValues != null) {
					var formDefaultValuesArray = formDefaultValues.split(",");
					for (var i = 0; i < formDefaultValuesArray.length; i++) {
						currentValue = formDefaultValuesArray[i];
						currentColumn = currentValue.split(":");
						if (currentColumn[0] != null && currentColumn[0] != '/') {
							var type = $("#" + currentColumn[0]).attr("type");
							if (type == 'checkbox') {
								if (currentColumn[1] == 'Y') {
									$("#" + currentColumn[0]).prop("checked", true);
								} else {
									$("#" + currentColumn[0]).prop("checked", false);
								}
							} else {
								$("#" + currentColumn[0]).val(currentColumn[1]);
							}

						}
					}
				}
				priceConroleOnAddOperation();
				var reciepientType = "OT";
				var panCharTop;
				panCharTop = $("#PAN_NUMBER").val();
				if (panCharTop && (panCharTop.charAt(3) == "C" || panCharTop.charAt(3) == "c")) {
					reciepientType = "CO";
				}
				$("#QSREC").val(reciepientType);
				/* end default values */
				//code for getting old data when clicked on add
				var tabsChangeOldObject = {};

				$("#" + tableName + "_TABLE" + " :input").each(function() {
					var textid = $(this).attr("id");
					var type = $(this).attr("type");
					var textval = $(this).val();
					if (type != 'hidden') {
						if (textval != null && textval != '') {
							textval = textval.toUpperCase();
						}
					}
					if (type != null && type == 'checkbox') {//
						if ($("#" + textid).is(':checked')) {
							textval = "Y";
						} else {
							textval = "N";
						}
					}
					//                  jsonOBJ.ids.push(textid.toLowerCase());
					if (textid != null && textid != 'CREATE_DATE') {
						tabsChangeOldObject[textid] = textval;
					}
				});
				if (tabsChangeOldObject != null) {
					tabsOldData[tableName] = tabsChangeOldObject;
				}
				setTimeout(changeflagFuction, 300);
				console.log(" add clicked change flag " + changeflag);
			}
			if (dataView == "GRID-VIEW") {
				$("#" + tableName + "_TABLE").hide();
				$("#" + tableName).show();
				dataView = "GRID-VIEW";
				$("#" + tableName + "_Update").attr("data-view", "GRID-VIEW");
				$("#" + tableName + "_Grid_View").show();
				//            $("#" + tableName + "_Delete").hide();
				$("#" + tableName + "_Update").show();

				// //console.log("click:::"+tableName);
				insertGridRow(tableName, dataView, tableName);
			}
		} else if (operation == "delete") {
			var wrappedData = [];
			var jsonOBJ = {};
			if (dataView == "FORM-VIEW") {
				ajaxStop();//11
				jsonOBJ.feildIds = [];
				jsonOBJ.feildValues = [];
				$("#" + tableName + "_TABLE :input").each(function() {
					var textid = $(this).attr("id");
					var type = $(this).attr("type");
					var textval = $(this).val();
					if (type != 'hidden') {
						if (textval != null && textval != '') {
							textval = textval.toUpperCase();
						}
					}
					if (textid.indexOf("AUDIT_ID")) {
						basicData[textid] = textval;
					}
					jsonOBJ.feildIds.push(textid);
					jsonOBJ.feildValues.push(textval);
				});
				jsonOBJ.basicData = basicData;
				ajaxStop();//12
				var results = "Do you sure you want to Delete this Record?";
				results = (labelObject[results] != null ? labelObject[results] : results);
				var dialogSplitMessage = dialogSplitIconText(results, "Y");
				$("#dialog").html(dialogSplitMessage);
				$("#dialog").dialog({
					modal: true,
					title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
					height: 'auto',
					minHeight: 'auto',
					minWidth: 350,
					maxWidth: 'auto',
					fluid: true,
					buttons: [{
						text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
						click: function() {
							ajaxStart();//2
							$(this).html("");
							$(this).dialog("close");
							var urlName = "deleteRecord";
							$(this).dialog("destroy");
							var jsonArray = [];
							jsonArray.push(jsonOBJ);
							UpdateOrDelete(JSON.stringify(jsonArray), dataView, tableName, operation);
						}
					},
					{
						text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
						click: function() {
							$(this).html("");
							$(this).dialog("close");
							$(this).dialog("destroy");
						}
					}],
					open: function() {
						$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
						$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
						$(this).closest(".ui-dialog").addClass("visionCommonDialog");
						$(".visionHeaderMain").css("z-index", "999");
						$(".visionFooterMain").css("z-index", "999");
					},
					beforeClose: function(event, ui) {
						$(".visionHeaderMain").css("z-index", "99999");
						$(".visionFooterMain").css("z-index", "99999");
					}
				});
			} else {
				var rowsSelected = [];
				var indexes = $("#" + tableName).jqxGrid('selectedrowindexes');
				if (indexes.length > 0) {
					var totalRowIndex = indexes.length;
					var datainformations = $('#' + tableName).jqxGrid('getdatainformation');
					//                    var datainformations = $('#' + gridId).jqxGrid('getdatainformation');
					if (datainformations != null) {
						var paginginformation = datainformations['paginginformation'];
						if (paginginformation != null) {
							var pagesize = paginginformation['pagesize'];
							if (pagesize != null && parseInt(pagesize) < totalRowIndex) {
								totalRowIndex = parseInt(pagesize);
							}
						}
					}
					for (var i = 0; i < totalRowIndex; i++) {
						var data = $("#" + tableName).jqxGrid('getrowdata', indexes[i]);
						//console.log(data);
						//data.boundindex = indexes[i];
						rowsSelected.push(data);
					}
				}
				alert("63fhdjh" + rowsSelected);
				if (rowsSelected == null || rowsSelected.length == 0) {
					ajaxStop();//13
					console.log("rowsSelected::::");
					var results = "No Record(s) to Delete";
					results = (labelObject[results] != null ? labelObject[results] : results);
					var dialogSplitMessage = dialogSplitIconText(results, "Y");
					$("#dialog").html(dialogSplitMessage);
					$("#dialog").dialog({
						modal: true,
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						height: 'auto',
						minHeight: 'auto',
						minWidth: 350,
						maxWidth: 'auto',
						buttons: [{
							text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
							click: function() {
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
								$("#" + tableName).jqxGrid('updatebounddata', 'cells');
								$("#" + tableName).jqxGrid('clearselection');
							}
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
							$(this).closest(".ui-dialog").addClass("visionCommonDialog");
						}
					});
				} else {
					ajaxStop();//14
					var results = "Do you sure you want to Delete this Record?";
					results = (labelObject[results] != null ? labelObject[results] : results);
					var dialogSplitMessage = dialogSplitIconText(results, "Y");
					$("#dialog").html(dialogSplitMessage);
					$("#dialog").dialog({
						modal: true,
						title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
						height: 'auto',
						minHeight: 'auto',
						minWidth: 350,
						maxWidth: 'auto',
						fluid: true,
						buttons: [{
							text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
							click: function() {
								ajaxStart();//3
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");

								var dataArray = [];
								console.log("rowsSelected::::" + JSON.stringify(rowsSelected));
								for (var i = 0; i < rowsSelected.length; i++) {

									var jsonData = {};
									obj = rowsSelected[i];
									jsonData.feildIds = [];
									jsonData.feildValues = [];

									for (var key in obj) {
										var value = obj[key];
										console.log("key:::::" + key + "::::::value::::" + value);
										jsonData.feildIds.push(key);
										jsonData.feildValues.push(value);
									}

									jsonData.basicData = basicData;
									dataArray.push(jsonData);
								}
								console.log("::::dataArray::::" + JSON.stringify(dataArray));
								UpdateOrDelete(JSON.stringify(dataArray), dataView, tableName, operation);
							}
						},
						{
							text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
							click: function() {
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
							}
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
							$(this).closest(".ui-dialog").addClass("visionCommonDialog");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});
				}
			}
		} else if (operation == "refresh") {

			ajaxStart();//4
			setTimeout(changeflagFuction, 300);
			if (dataView == "FORM-VIEW") {
				if (tableName != null && tableName.indexOf("ERP") > -1) {
					fetchErpTab(tableName, '');
				} else {
					fetchTabData(tableName);
				}
			}
			if (dataView == "TABLE-VIEW") {
				var opName = $("#" + tableName).val();
				if (opName == 'INSERT') {
					selectedDataArray = gridOperation("insert", tableName);
				} else {
					selectedDataArray = gridOperation("update", tableName);
				}
				if (selectedDataArray.length != 0) {
					ajaxStop();//15
					var results = "Do you want to save your changes?";
					results = (labelObject[results] != null ? labelObject[results] : results);
					var dialogSplitMessage = dialogSplitIconText(results, "Y");
					$("#dialog").html(dialogSplitMessage);
					$("#dialog").dialog({
						modal: true,
						title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
						height: 'auto',
						minHeight: 'auto',
						minWidth: 350,
						maxWidth: 'auto',
						fluid: true,
						buttons: [{
							text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
							click: function() {
								ajaxStart();//5
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
								endoperation(selectedDataArray, tableName, dataView, operation, basicData);
							}
						},
						{
							text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
							click: function() {
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
								if (tableName != null && tableName.indexOf("ERP") > -1) {
									fetchErpTab(tableName, '');
								} else {
									fetchTabData(tableName);
									$(tableName).jqxGrid('clearselection');
								}
							}
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
							$(this).closest(".ui-dialog").addClass("visionCommonDialog");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});
					ajaxStop();//16

				} else {
					//                 $("#grid").jqxGrid('updatebounddata', 'cells');
					$("#" + tableName).jqxGrid('updatebounddata', 'cells');
					$("#" + tableName).jqxGrid('clearselection');
					//                    $("#" + tableName).jqxGrid('clearfilters');
					ajaxStop();//17
				}
			} else if (dataView == "GRID-VIEW") {
				var editable = $("#" + tableName).jqxGrid('editable');
				if (editable) {
					var opName = $("#" + tableName).val();
					if (opName == 'INSERT') {
						selectedDataArray = gridOperation("insert", tableName);
					} else {
						selectedDataArray = gridOperation("update", tableName);
					}
					if (selectedDataArray.length != 0) {
						ajaxStop();//18
						var results = "Do you want to save your changes?";
						results = (labelObject[results] != null ? labelObject[results] : results);
						var dialogSplitMessage = dialogSplitIconText(results, "Y");
						$("#dialog").html(dialogSplitMessage);
						$("#dialog").dialog({
							modal: true,
							title: (labelObject['Confirmation'] != null ? labelObject['Confirmation'] : 'Confirmation'),
							height: 'auto',
							minHeight: 'auto',
							minWidth: 350,
							maxWidth: 'auto',
							fluid: true,
							buttons: [{
								text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
								click: function() {
									ajaxStart();//6
									$(this).html("");
									$(this).dialog("close");
									$(this).dialog("destroy");
									endoperation(selectedDataArray, tableName, dataView, operation, basicData);
								}
							},
							{
								text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
								click: function() {
									$(this).html("");
									$(this).dialog("close");
									$(this).dialog("destroy");
									if (tableName != null && tableName.indexOf("ERP") > -1) {
										fetchErpTab(tableName, '');
									} else {
										if (tableName.indexOf("DESCRIPTIONS") > -1) {
											refreshDecriptionTab(tableName);
										} else {
											fetchTabData(tableName);
											$(tableName).jqxGrid('clearselection');
										}
									}
								}
							}],
							open: function() {
								$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
								$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
								$(this).closest(".ui-dialog").addClass("visionCommonDialog");
								$(".visionHeaderMain").css("z-index", "999");
								$(".visionFooterMain").css("z-index", "999");
							},
							beforeClose: function(event, ui) {
								$(".visionHeaderMain").css("z-index", "99999");
								$(".visionFooterMain").css("z-index", "99999");
							}
						});
					} else {
						$("#" + tableName).jqxGrid('updatebounddata', 'cells');
						$("#" + tableName).jqxGrid('clearselection');
						ajaxStop();//19
					}
				} else {
					if (tableName.indexOf("DESCRIPTIONS") > -1) {
						refreshDecriptionTab(tableName);
					} else {
						fetchTabData(tableName);
					}
				}
			}
		} else if (operation == "Grid_View") {
			fetchTabData(tableName);
		} else if (operation == "audit") {
			var clauseColumns = $("#" + tableName + "_audit").attr("clauseColumns");
			navigateToAuditView(tableName, basicDataAudit, clauseColumns);
		}
	}
	///fetching generically
}
function psCount(tabId) {
	var psTotalCount = 0;
	var psMTMandCount = 0;
	var psMTEnteredCount = 0;
	var psEnteredCount = 0;
	var lblString = "";
	var avoidString = "lblMand";
	var lblFieldId = "";
	var lblMultiFieldId = "";
	var dataType = "";
	var splitCount = 0; // for multi text's
	$("#" + tabId + "_TABLE tr th:even sup").each(function() {
		if ($(this).css("display") != "none") {
			psTotalCount++;
			lblString = $(this).attr("class");
			lblFieldId = lblString.replace(avoidString, '');
			dataType = $("#" + lblFieldId).attr("data-type");
			if ($("#" + lblFieldId).val() && dataType != "MT") {
				psEnteredCount++;
			}
			if (dataType == "MT") {
				splitCount = $("#" + lblFieldId).attr("splitcount");
				for (var i = 1; i <= splitCount; i++) {
					lblMultiFieldId = lblFieldId + i;
					($("#" + lblMultiFieldId).attr("data-mandatory") == "M") ? psMTMandCount++ : "";
					if (($("#" + lblMultiFieldId).attr("data-mandatory") == "M") && ($("#" + lblMultiFieldId).val() != "")) {
						psMTEnteredCount++;
					}
				}
				(psMTMandCount <= psMTEnteredCount) ? psEnteredCount++ : "";
			}
		}
	});
	//                alert("psTotalCount::"+psTotalCount);
	if (psTotalCount > 0)
		$("#" + tabId + "_MO_COUNT").text(psEnteredCount + "/" + psTotalCount);
	else
		$("#" + tabId + "_MO_COUNT").text("");
}
function disableOrEnableWthTanTabAttribuetes() {
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	var lowerTDSValue = $("#TDS_APPL").val();
	if (lowerTDSValue == "Y" || lowerTDSValue == "y") {
		$("#VM_WTH_TAN_DATA_Update").show();
		$("table#" + "VM_WTH_TAN_DATA" + "_TABLE").each(function() {
			$(this).find("img").show();
		});
		$("table#" + "VM_WTH_TAN_DATA" + "_TABLE :input").each(function() {
			var id = $(this).attr('id');
			if (id !== "WT_EXNR_TAN" && id != "WT_EXRT_TAN" && id != "FIWTIN_EXEM_THR") {
				//                $("#" + id).attr("disabled", true);
				$("#" + id).attr("readonly", true);

			} else {
				//                $("#" + id).attr("disabled", false);
				$("#" + id).attr("readonly", false);
			}
		});
	} else if (lowerTDSValue == "N" || lowerTDSValue == "n") {
		$("#VM_WTH_TAN_DATA_Update").hide();
		$("table#" + "VM_WTH_TAN_DATA" + "_TABLE").each(function() {
			//            $(this).find("input").attr('disabled', true);
			$(this).find("input").attr('readonly', true);
			$(this).find("img").hide();
		});
	}

}
function erpTab(tabId, erpDataObj, erpDataFlag, formView) {
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {

	}
	tabsOldData = {};
	if (erpDataObj != null) {
		if (erpDataObj['tabGridId'] != null && erpDataObj['tabGridId'] != '' && erpDataObj['tabGridId'].indexOf('_OLD') < 0) {
			globalErpTab = erpDataObj['tabGridId'];
		}
		if (erpDataFlag == 'Y') {
			var erpGridDataObj = erpDataObj['erpGridResults'];
			if (erpGridDataObj != null) {
				var erpGridConfig = erpGridDataObj['gridPropObj'];
				console.log("erpGridDataObj['tableData']::::" + JSON.stringify(erpGridDataObj['tableData']));
				console.log("erpGridDataObj['columns']::::" + JSON.stringify(erpGridDataObj['columns']));
				var source =
				{
					datatype: "json",
					localdata: erpGridDataObj['tableData'],
					datafields: erpGridDataObj['datafields']

				};
				var dataAdapter = new $.jqx.dataAdapter(source);
				erpGridConfig.source = dataAdapter;
				var renderToolbar = erpGridConfig.renderToolbar;
				erpGridConfig.renderToolbar = eval('(' + renderToolbar + ')');
				erpGridConfig.columns = erpGridDataObj['columns'];

				var headerTooltipRenderer = function(element) {
					$(element).parent().jqxTooltip({
						position: 'mouse',
						position: 'bottom-right',
						showArrow: false, content: $(element).text()
					});
				}
				var dataSheetRendered = function(element) {

					// $(element).html("<div class='show_detail' ></div>");
					$(element).addClass("show_detail");
					$(element).parent().jqxTooltip({
						position: 'mouse',
						position: 'bottom-right',
						showArrow: false,
						content: "Data Sheet"
					});
					//content: $(element).text()});
				}
				var descrender = function(row, columnfield, value, defaulthtml, columnproperties) {

					return '<div style="height:' + $("#" + tabId).jqxGrid('rowsheight') + 'px" class="ta_style ta_style_Desc"  ><pre>' + value + '</pre></div>';
				};

				var gridDrpdownRenderor = function(row, columnfield, value, defaulthtml, columnproperties) {
					var cellValue = $("#" + tabId + "_TABLE").jqxGrid('getcellvalue', row, columnfield);
					var viewType = "GRID-VIEW";
					var ddwData = erpGridConfig.dropDowndData;
					var ddwObj = ddwData[columnfield];
					var dependencyparams = ddwObj['dependencyparams'];
					var editable = erpGridConfig['editable'];
					if (editable) {
						return "<div class='visionGridDataAlign'><div class='visionGridDataAlignInfo'>" + cellValue + "</div><div class='visionGridDataAlignImage'><img id='dd" + tabId + "_TABLE" + columnfield + "' src='images/search_icon_color_2.png' onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
						//                        return "<div class='visionGridDataAlign'><div class='visionGridDataAlignInfo'>" + cellValue + "</div><div class='visionGridDataAlignImage'><img src='images/search_icon_color_2.png' onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
					} else {
						return "<div class='visionGridDataAlign'>" + cellValue + "</div>";
					}
				};
				//                erpGridConfig.enabletooltips = false;
				erpGridConfig.cellhover = function(element, pageX, pageY) {
				};
				for (var i = 0; i < erpGridConfig.columns.length; i++) {
					if (erpGridConfig.columns[i].cellsrenderer != null) {
						erpGridConfig.columns[i].cellsrenderer = eval(erpGridConfig.columns[i].cellsrenderer);
					}
					if (erpGridConfig.columns[i].rendered != null) {
						erpGridConfig.columns[i].rendered = eval('(' + erpGridConfig.columns[i].rendered + ')');
					}
				}
				var paginationFlag = erpGridConfig['pageable'];
				if (paginationFlag) {
					erpGridConfig.virtualmode = false;

				}
				if (erpGridConfig['rowsheight'] != null && erpGridConfig['rowsheight'] != '') {//rowsheight
					erpGridConfig.autorowheight = true;
				}
				$('#' + tabId + "_TABLE").jqxGrid(erpGridConfig);
				$('#' + tabId + "_TABLE").on('celldoubleclick', function(event) {
					var args = event.args;
					var dataField = args.datafield;
					var dataField1 = args.text;
					var rowIndex = args.rowindex;
					var cellValue = args.value;
					var isEditable = $('#' + tabId + "_TABLE").jqxGrid('getcolumnproperty', dataField, 'editable');
					console.log("isEditable::::" + isEditable)
					if (!isEditable) {
						var column = $('#' + tabId + "_TABLE").jqxGrid('getcolumn', event.args.datafield).text;
						popupedit(column, cellValue);
					}

				});
			}

			alert("tabId::::" + tabId);
			if ($('#' + tabId).html() != "") {
				$('#' + tabId).jqxTabs('destroy');
				$('#' + tabId + "_TABLE").after('<div id="' + tabId + '"></div>');
			}
			$('#' + tabId).html(erpDataObj['tabString']);
			//                                        $('#erpData').html(erpDataObj['tabString']);
			$('#' + tabId).html(erpDataObj['tabString']);
			$('#' + tabId).jqxTabs({ position: 'top', width: '100%', reorder: true, theme: 'ui-redmond', keyboardNavigation: true });

			$('#' + erpDataObj['tabGridId'] + "Icon").html(erpDataObj['tabOperationIcon']);
			//   var erpTabGridId = $("#erpTabGridId").val();
			if (erpDataObj['erpTabGridId'] != null && erpDataObj['erpTabGridId'] != '' && (erpDataObj['erpTabGridId']).toString().indexOf("_OLD") == -1) {
				//  $("#erpTabGridId").val(erpDataObj['erpTabGridId']);
				$("#erpTabGridId").val(erpDataObj['erpTabGridId']);
			}
			if ($("#" + erpDataObj['erpTabGridId'] + "HiddenGridData").length == 0) {
				$("#mat_creation_form_table").append("<input type='hidden' id='" + erpDataObj['erpTabGridId'] + "HiddenGridData' value='' />");
			}
			$("#" + erpDataObj['erpTabGridId'] + "HiddenGridData").val(erpDataObj['gridIds']);
			alert("erpDataGridId:::After::" + $("#erpDataGridId").val());


			var unSelectTab = "";
			var gridIds = $("#" + tabId + "HiddenGridData").val();
			var gridIdsArry = gridIds.split(",");
			//            $('#' + tabId).on('unselecting', function (event) {
			//                unSelectTab = event.args.item;
			//            });

			var UnselectedGridId = "";
			var matchedcount = 0;
			$('#' + tabId).on('unselecting', function(event) {

				UnselectedGridId = gridIdsArry[event.args.item];
				console.log("before matchedcount : " + matchedcount);
				console.log("change matchedcount : " + changeflag);
				console.log("UnselectedGridId : " + UnselectedGridId);

				var jsonOBJ = {};
				jsonOBJ.feildIds = [];
				jsonOBJ.feildValues = [];
				matchedcount = 0;
				console.log("after matchedcount : " + matchedcount);

				var dataView = $("#" + UnselectedGridId + "_Update").attr("data-view");
				var selectedTabOldData = tabsOldData[UnselectedGridId];

				if (dataView == "FORM-VIEW") {
					changeflag = false;
					$("[id*=" + UnselectedGridId + "]  :input").each(function() {
						var textid = $(this).attr("id");
						var type = $(this).attr("type");
						var textval = $(this).val();
						//                console.log("textid:::" + textid);
						if (type != 'hidden') {
							if (textval != null && textval != '') {
								textval = textval.toUpperCase();
							}
						}


						if (type != null && type == 'checkbox') {//
							if ($("#" + textid).is(':checked')) {
								textval = "Y";
							} else {
								textval = "N";
							}
						}


						var textOldVal = "";
						if (selectedTabOldData != null) {
							textOldVal = selectedTabOldData[textid];
							if (textid != null && textid != 'CREATE_DATE' && textval != textOldVal) {
								matchedcount++;
							}
						}
						//                console.log(textval + ":::" + textid + "::" + textOldVal);

						if (matchedcount > 0) {
							changeflag = true;
						}


					});

				}

				if (dataView == "GRID-VIEW") {
					matchedcount = 1;
					console.log(" GRID-VIEW CODE");
					var changecount = 0;
					$("[id^=contenttable]  :input").each(function() {
						//            var textid = $(this).attr("id");
						var type = $(this).attr("type");
						var textval = $(this).val();
						console.log(" type : " + type + " textval : " + textval + " cell old value " + cellOldValue);

						if (type == 'textbox' && textval != null && textval != cellOldValue) {
							changecount++;
						}
					});
					if (changecount > 0) {
						console.log(" grid input changed ccount " + changecount);
						changeflag = true;
						console.log("grid input changed : changeflag = " + changeflag);
					}
				}


			});
			$('#' + tabId).on('selecting', function(event) {
				var baskettype = $('#' + tabId).jqxTabs('getTitleAt', unSelectTab);
				var unselectedGridId = gridIdsArry[unSelectTab];
				var selectedGridId = gridIdsArry[event.args.item];
				var erpTab = tabId;
				if (changeflag && matchedcount > 0) {
					if (tabSwitchflag) {
						event.cancel = true;
						// event.preventDefault();
					}
					$("#logoutDailog").html((labelObject['Unsaved changes will be lost'] != null ? labelObject['Unsaved changes will be lost'] : 'Unsaved changes will be lost') + ", " + (labelObject['you like to save'] != null ? labelObject['you like to save'] : 'you like to save') + "?");
					$("#logoutDailog").dialog({
						title: (labelObject['Error'] != null ? labelObject['Error'] : 'Error'),
						modal: true,
						width: 300,
						height: 135,
						fluid: true,
						buttons: [
							{
								text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
								click: function() {
									$(this).html("");
									$(this).dialog("close");
									$(this).dialog("destroy");
								}
							}, {
								text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
								click: function() {

									$(this).html("");
									$(this).dialog("close");
									tabSwitchflag = false;
									var selectedTab = event.args.item;
									console.log("selectedTab  :: " + selectedTab);
									changeflag = false;
									$('#' + tabId).jqxTabs('select', selectedTab);
									fetchErpTab(selectedGridId, erpTab);
									$(this).dialog("close");
									changeflag = false;
									tabSwitchflag = true;
									console.log(length + " tabSwitchflag  : " + tabSwitchflag);

								}
							}
						],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});

				}
				if (!changeflag) {
					fetchErpTab(selectedGridId, erpTab);
				} else if (tabId != null && tabId.indexOf("_OLD") > 0) {
					fetchErpTab(selectedGridId, erpTab);
				}
			});
			$('#' + tabId + "_TABLE").on('rowselect', function(event) {
				var rows = $('#' + tabId + "_TABLE").jqxGrid('getrows');
				// alert(rows.length);
				var index = event.args.rowindex;
				for (var i = 0; i < rows.length; i++) {
					if (i != index) {
						try {
							var bindex = $('#' + tabId + "_TABLE").jqxGrid('getrowboundindex', i);
							$('#' + tabId + "_TABLE").jqxGrid('unselectrow', bindex);
						} catch (err) {
						}
					}
				}
				refreshErpTab(rows[event.args.rowindex], tabId);
			});
			if (formView == 'FORM-VIEW') {
				$('#' + erpDataObj['tabGridId'] + "_FORM").html(erpDataObj['data']);
				$("#" + erpDataObj['tabGridId'] + "_FORM" + " :input[data-type='D']").each(function() {
					var id = $(this).attr('id');
					var isEditable = $("#" + id).attr('data-editable');
					if (isEditable == "Y") {
						$("#" + id).datepicker({
							changeMonth: true,
							changeYear: true,
							dateFormat: "dd-mm-yy",
							showOn: "button",
							buttonImage: 'images/date_picker_icon.png',
							buttonImageOnly: true
						});
					}
				});
				var tabOldObj = {};

				$("#" + erpDataObj['tabGridId'] + "_TABLE" + " :input").each(function() {
					var textid = $(this).attr("id");
					var type = $(this).attr("type");
					var textval = $(this).val();
					if (type != 'hidden') {
						if (textval != null && textval != '') {
							textval = textval.toUpperCase();
						}
					}
					if (type != null && type == 'checkbox') {//
						if ($("#" + textid).is(':checked')) {
							textval = "Y";
						} else {
							textval = "N";
						}
					}
					if (textid != null && textid != 'CREATE_DATE') {
						tabOldObj[textid] = textval;
					}
				});
				if (tabOldObj != null) {
					tabsOldData[erpDataObj['tabGridId']] = tabOldObj;
				}
			} else {
				formGrid(erpDataObj['tabGridId'], erpDataObj, erpDataFlag);
			}
		} else {
			formGrid(tabId, erpDataObj, erpDataFlag);
		}
	}
}// end of erpTab()
function UpdateOrDelete(data, dataView, tabId, operation) {
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	console.log("UpdateOrDelete::: check data" + tabId + ":::" + operation);
	console.log(data);
	var jsondata = {};
	var basicData = {};
	var reviewIndFV;
	var vendorCode = $("#vendorCode").val();
	var locatCode = $("#locatcode").val();
	var companyCode = $("#compCode").val();
	var accountGroup = $("#accountGroup").val();
	var purchaseOrg = $("#purchOrg").val();
	var purchaseOrg = $("#purchOrg").val();
	var baskettype = $('#baskettypehid').val();
	var requestNumber = $("#requestNumber").val();
	var vendorCode = $("#vendorCode").val();
	if ($('#foreignReviewIndicator').is(':checked')) {
		reviewIndFV = "Y";
	} else {
		reviewIndFV = "N";
	}
	var reviewIndCA = "";
	if ($('#caReviewIndicator').is(':checked')) {
		reviewIndCA = "Y";
	} else {
		reviewIndCA = "N";
	}
	var newIfsc = "";
	if ($('#NEW_BNK').is(':checked')) {
		newIfsc = "Y";

	} else {
		newIfsc = "N";
	}
	$("#mat_creation_form_table :input").each(function() {
		var textid = $(this).attr("id");
		var textval = "";
		if ($("#" + textid).val() !== null && $("#" + textid).val() !== "") {
			var type = $(this).attr("type");
			textval = $(this).val();
			if (type != 'hidden') {
				if (textval != null && textval != '') {
					textval = textval.toUpperCase();
				}
			}
		}
		if (textid != null && textid != 'CREATE_DATE') {
			basicData[textid] = textval;
		}
		if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
			var columnNames = $("#" + textid).val();
			var columnsArray = columnNames.split(",");

			var hiddenIds = textid.split("HIDDEN_");
			var hiddenVal = $("#" + hiddenIds[1]).val();
			for (var i = 0; i < columnsArray.length; i++) {
				if (hiddenVal != null) {
					hiddenVal = hiddenVal.toUpperCase();
				}
				basicData[columnsArray[i]] = hiddenVal;
			}
		}
	});
	console.log(JSON.stringify(basicData));
	var jsonOBJ = {};
	var dataArray = [];
	var finalData = "";
	console.log("basicData::::" + JSON.stringify(basicData));
	if (dataView != "GRID-VIEW") {
		jsonOBJ = JSON.parse(data);
		jsonOBJ.basicData = basicData;
		dataArray.push(jsonOBJ);
		finalData = JSON.stringify(jsonOBJ);
	} else {
		jsonOBJ = {};
		var gridData = JSON.parse(data);
		finalData = JSON.stringify(gridData);
	}
	var url = "";

	if (operation == "update" || operation == 'checkingTabData') {
		url = "updateRecord";
	} else if (operation == "delete") {
		url = "deleteRecord";
	} else if (operation == "calculateStock") {
		//        fetchCalculateStock(finalData, tabId, dataView);
	}
	if (operation != 'calculateStock') {
		var reqNumber = $("#REQ_NUMBER").val() != null ? $("#REQ_NUMBER").val() : "";
		var status = $("#STATUS").val() != null ? $("#STATUS").val() : "";
		$.ajax({
			type: "POST",
			url: url,
			data: {
				dataView: dataView,
				jsonData: finalData,
				gridId: tabId,
				panelId: $("#panelId").val(),
				'STATUS': status,
				'REQ_NUMBER': reqNumber,
				checkAttachType: ($("#checkAttachType").val() != null ? $("#checkAttachType").val() : ""),
				initParamSource: ($("#initParamSource").val() != null ? $("#initParamSource").val() : "")
			},
			traditional: true,
			cache: false,
			success: function(result) {
				var resultMessage;
				var response = JSON.parse(result);
				// var resultNew = response.resultVal;
				var resultNew = response.Message;
				var flag = response.messageFlag;
				if (result == null || result == "") {
					result = "Failed to Update!"
					result = (labelObject[result] != null ? labelObject[result] : result);

				}
				var hiddenGridId = $('#' + tabId + "_HIDDEN").val();
				if (hiddenGridId != null && hiddenGridId == "INSERT" && operation == "delete" && resultNew.lastIndexOf("Failed") > -1) {
					resultMessage = "No Record to Delete.";
					resultMessage = (labelObject[resultMessage] != null ? labelObject[resultMessage] : resultMessage);
				} else {
					resultMessage = response.Message;
				}
				if (operation == 'checkingTabData') {
					checkingTabData(tabId, basicData, dataView);
				} else {
					ajaxStop();//23
					var dialogSplitMessage = dialogSplitIconText(resultMessage, flag);
					$("#dialog").html(dialogSplitMessage);
					$("#dialog").dialog({
						modal: true,
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						height: 'auto',
						minHeight: 'auto',
						minWidth: 300,
						maxWidth: 'auto',
						fluid: true,
						buttons: [{
							text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
							click: function() {
								if (flag) {
									// if (result.lastIndexOf("Successfully") > -1 || result.lastIndexOf("successfully") > -1) {
									alert("Successfully");
									if (tabId != null && tabId.indexOf("ERP") > -1) {
										fetchErpTab(tabId, '');
									} else if (tabId != null
										&& (tabId.indexOf("MM_ATTACHMENTS") > -1
											|| tabId.indexOf("SM_ATTACHMENTS") > -1
											|| tabId.indexOf("SPEC_ATTACHMENTS") > -1)) {
										fetchAttachmentsTabGridData(tabId);
									} else {
										fetchTabData(tabId, '');
										var role = $("#rolehid").val();
										// GenerateInstantDescription(tabId.indexOf("ERP") == -1 && tabId.indexOf("ATTACH") == -1 && role != null && ((role.indexOf("MM") == 0) || (role.indexOf("SM") == 0)));
									}
								} else {
									if (dataView == "GRID-VIEW") {
										if (tabId != null &&
											(tabId.indexOf("MM_ATTACHMENTS") > -1
												|| tabId.indexOf("SM_ATTACHMENTS") > -1
												|| tabId.indexOf("SPEC_ATTACHMENTS") > -1)) {
											fetchAttachmentsTabGridData(tabId);
											$('#' + tabId).jqxGrid('clearselection');
										} else if (tabId != null && tabId.indexOf("ERP") > -1) {
											fetchErpTab(tabId, '');
										} else {
											fetchTabData(tabId);
											$('#' + tabId).jqxGrid('clearselection');
										}
									} else if (dataView == "FORM-VIEW") {
										if (hiddenGridId != null && hiddenGridId == "INSERT" && operation == "delete") {
											fetchTabData(tabId);
										}
									}
								}

								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
							}
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});

					if (tabId != null && tabId.indexOf("GENERAL") > -1 && flag) {
						//                if (dialogSplitMessage.lastIndexOf("Update") > -1 && dialogSplitMessage.lastIndexOf("Success") > -1) {
						var gstCodeTax;
						gstCodeTax = $("#GST_CODE_GEN").val();
						$("#GST_CODE_BASE").val(gstCodeTax);
						//                }
					}
					if (tabId != null && tabId.indexOf("TAXATION") > -1 && flag) {
						//                if (dialogSplitMessage.lastIndexOf("Update") > -1 && dialogSplitMessage.lastIndexOf("Success") > -1) {
						var reciepientType = "OT";
						var panCharTop, panCharTax;
						panCharTax = $("#O_1IPANNO").val();
						$("#PAN_NUMBER").val(panCharTax);
						panCharTop = $("#PAN_NUMBER").val();
						if (panCharTop && panCharTop.charAt(3) == "C") {
							reciepientType = "CO";
						}
						$("#QSREC").val(reciepientType);
						//                }
					}
				}
			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
			}
		});
	}
	console.log("withholdingTanUpdate ::: END");
	setTimeout(changeflagFuction, 300);
}// updateOrDelete fun
function formGrid(tabId, jsnobj, erpDataFlag) {
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	//var erpDataFlag = jsnobj['erpData'];
	console.log("erpDataFlag::::" + erpDataFlag);
	$("#" + tabId + "_Update").attr("data-view", "GRID-VIEW");
	$("#" + tabId + "_Delete").attr("data-view", "GRID-VIEW");
	$("#" + tabId).show();
	$("#" + tabId + '_TABLE').hide();
	var columns = jsnobj.columns;
	var datafields = jsnobj.datafields;
	var localdata = jsnobj.data;
	var dropDownListData = jsnobj.dropDownListData;
	grioldDataObj.oldData = localdata;
	var tableName = "";
	if (jsnobj['panelData'] != null && jsnobj['panelData'][13] != null) {
		tableName = jsnobj['panelData'][13];
	}
	var columnInitParamsObj = jsnobj['columnInitParamsObj'];
	var listTypeColName = [];
	var listTypeColNameId = [];
	var newLocalData = [];
	var gridConfigObj = {};
	var gridPropObj = {};
	gridPropObj = jsnobj.gridPropObj;
	var renderToolbar = gridConfigObj.renderToolbar;
	var gridInitParamObj = {};
	gridInitParamObj = jsnobj['gridInitParamObj'];
	gridConfigObj.renderToolbar = eval('(' + renderToolbar + ')');
	$("#" + tabId + "_DIV").html("<div id='" + tabId + "'></div>");
	var dateRenderer = function(row, columnfield, value, defaulthtml, columnproperties, rowData) {
		var cellValue = $("#" + tabId).jqxGrid('getcellvalue', row, columnfield);
		// console.log("cellValue::"+cellValue);
		if (cellValue != null && cellValue != '') {
			var dateValue = $.jqx.dataFormat.formatdate(value, 'dd-MM-yyyy', $("#" + tabId).jqxGrid('gridlocalization'));
			//console.log("dateValue:::"+dateValue);
			cellValue = dateValue;
		}
		return cellValue;
	};
	var headerTooltipRenderer = function(element) {
		$(element).parent().jqxTooltip({
			position: 'mouse',
			position: 'bottom-right',
			showArrow: false, content: $(element).text()
		});
	}
	var attachmentImageRenderer = function(row, columnfield, value, defaulthtml, columnproperties) {

		if (value != "" && value != null) {
			// return value;
			// $( ".visionGridColFileClass" ).tooltip({ content: 'click to show' });
			return "<img title='Click to view the attachment' style='cursor:pointer;' onclick=viewAttachment('" + tabId + "'," + row + ",'" + tableName + "')  src='" + value + "' class='imageStyle visionTemplete'  id='dtlul_" + row + "' >";

		} else {
			return "<div class='visionCoFileImage'>"
				+ "<input name='colFileImage' type='file' id ='visionColFileId' style ='display:none'/>"
				+ "<img src='images/attach_pin_icon_blue.png' onclick=showBrowseIdButton('" + tabId + "') style='cursor:pointer;margin-left: 30%;'/>"
				+ "</div>";

		}
	};
	var dataSheetRendered = function(element) {
		$(element).parent().jqxTooltip({
			position: 'mouse',
			position: 'bottom-right',
			showArrow: false, content: $(element).text()
		});
	}
	var gridDrpdownRenderor = function(row, columnfield, value, defaulthtml, columnproperties) {
		var cellValue = $("#" + tabId).jqxGrid('getcellvalue', row, columnfield);

		var viewType = "GRID-VIEW";
		var editable = gridConfigObj.editable;
		console.log("===============================================");
		console.log("Inside gridDrpdownRenderor editable::" + editable)
		console.log("===============================================");
		if (columnInitParamsObj != null && columnInitParamsObj != '' && columnInitParamsObj != undefined) {
			var columnParams = columnInitParamsObj[columnfield];
			if (columnParams != null && columnParams != '' && columnParams != undefined) {
				var editableFlag = columnParams['uuu_editable'];
				var hiddenType = $('#' + tabId).jqxGrid('getcellvalue', row, tabId + "_HIDDEN");
			}
		}
		if (editable) {
			if (editableFlag != null && editableFlag != '' && editableFlag == "N") {
				if (hiddenType != null && hiddenType != '' && hiddenType != undefined && hiddenType != "INSERT") {
					var ddwData = jsnobj.dropDowndData;
					var ddwObj = ddwData[columnfield];
					var dependencyparams = ddwObj.dependencyparams;
					return "<div  class='visionGridDataAlign' >" + cellValue + "</div>";
				} else {
					var ddwData = jsnobj.dropDowndData;
					var ddwObj = ddwData[columnfield];
					var dependencyparams = ddwObj.dependencyparams;
					$("#" + tabId).jqxGrid('setcolumnproperty', columnfield, 'editable', false);
					//return "<div  style='width:99.5%;vertical-align:middle;height:100%;padding:2px 12px 2px 3px;' >" + cellValue + "<img class='prop_imgClass' src='images/search_icon_color_2.png' style='width:15px;height:15px;float:right;' onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div>";
					return "<div  class='visionGridDataAlign'><div class='visionGridDataAlignInfo'> " + cellValue + "</div><div class='visionGridDataAlignImage'><img id='dd" + tabId + columnfield + "' src='images/search_icon_color_2.png'  onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
				}
			} else {
				var ddwData = jsnobj.dropDowndData;
				var ddwObj = ddwData[columnfield];
				var dependencyparams = ddwObj.dependencyparams;
				$("#" + tabId).jqxGrid('setcolumnproperty', columnfield, 'editable', false);
				//return "<div  style='width:99.5%;vertical-align:middle;height:100%;padding:2px 12px 2px 3px;' >" + cellValue + "<img class='prop_imgClass' src='images/search_icon_color_2.png' style='width:15px;height:15px;float:right;' onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div>";
				return "<div  class='visionGridDataAlign'><div class='visionGridDataAlignInfo'> " + cellValue + "</div><div class='visionGridDataAlignImage'><img src='images/search_icon_color_2.png'  onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "','" + row + "')></div></div>";
			}

		} else {
			var ddwData = jsnobj.dropDowndData;
			var ddwObj = ddwData[columnfield];
			var dependencyparams = ddwObj.dependencyparams;
			return "<div  class='visionGridDataAlign' >" + cellValue + "</div>";
		}
	};
	/* Renderer for textbox with dropdown*/
	var TB_DDW = function(row, columnfield, value, defaulthtml, columnproperties) {
		console.log("Entered TB_DDW renderer");
		//   var editable = response.gridPropObj.editable;
		var ddwData = jsnobj.dropDowndData;
		console.log("ddwData::" + JSON.stringify(ddwData));
		var ddwObj = ddwData[columnfield];
		var dependencyparams = ddwObj.dependencyparams;
		var tbid = ddwObj.gridId + row;
		value = $("#" + ddwObj.gridId).jqxGrid('getcellvalue', row, 'PROPERTY_VALUE1');
		console.log("renderer::" + row + "::" + value);
		var viewType = "GRID-VIEW";
		if (value == null || value == 'null') {
			value = "";
		}
		return "<div  class='visionGridDataAlignInput' data-recid='' data-prop=''>"
			//  + "<input type='text' style='width:100%;' value='" + value + "' id='" + ddwObj.gridId + row + "'>"
			+ "<div class='visionGridDataAlignInputField'>"
			+ "<input type='text'"
			+ " onkeyup=propValKeyUp1('" + tbid + "'," + row + ",'none','" + ddwObj.gridId + "','" + columnfield + "',event)"
			+ " value='" + value + "' id='" + ddwObj.gridId + row + "'/>"
			+ "</div><div class='visionGridDataAlignInputImage'>"
			+ " <img src='images/icon.png' "
			+ " onclick=visionDropdown('" + ddwObj.ddwId.trim() + "','" + dependencyparams + "','" + viewType + "','" + ddwObj.gridId + "','" + columnfield + "'," + row + ",'" + ddwObj.gridId + row + "')></div></div>";

	};
	var descrender = function(row, columnfield, value, defaulthtml, columnproperties) {

		return '<div style="height:' + $("#" + tabId).jqxGrid('rowsheight') + 'px" class="ta_style ta_style_Desc"  ><pre>' + value + '</pre></div>';
	};
	var descsaprender = function(row, columnfield, value, defaulthtml, columnproperties) {

		return '<div style="height:' + $("#" + tabId).jqxGrid('rowsheight') + 'px" class="ta_style ta_style_Desc"  ><pre>' + value + '</pre></div>';
	};
	if (gridPropObj.rowsheight != null) {
		gridPropObj.rowsheight = parseInt(gridPropObj.rowsheight);
		// gridPropObj.autorowheight = true;
	}
	var viewLocationRenderer = function(row, columnfield, value, defaulthtml, columnproperties) {
		//return '<textarea readonly class="ta_style" rows=1 >' + value + '</textarea>';
		console.log("hiiiii");
		return '<div class="nestedGridBorrowButton nestedGridBuyBorrowCol" onclick=initialize("' + row + '") style="cursor:pointer;"><img src="images/location.png">&nbsp;Location</div>';
	};
	var viewAddressRenderer
		= function(row, columnfield, value, defaulthtml, columnproperties) {
			//return '<textarea readonly class="ta_style" rows=1 >' + value + '</textarea>';
			console.log("hiiiii");
			return '<div class="nestedGridBorrowButton nestedGridBuyBorrowCol" onclick=getViewColumnForm("' + row + '") style="cursor:pointer;"><img src="images/address.png">&nbsp;Vendor Info</div>';
		};
	var charRenderer = function(row, columnfield, value, defaulthtml, columnproperties) {
		var tooltip = "";
		var ddwData = jsnobj.dropDowndData;
		console.log("ddwData::" + JSON.stringify(ddwData));
		var ddwObj = ddwData[columnfield];
		var dependencyparams = ddwObj.dependencyparams;
		var property = value;
		var mand_ind;
		var highlevelid;
		mand_ind = $('#' + ddwObj.gridId).jqxGrid('getcellvalue', row, "REQUIRED_FLAG");
		highlevelid = $('#' + ddwObj.gridId).jqxGrid('getcellvalue', row, "HIGH_LEVEL_FLAG");
		console.log("highlevelid:::" + highlevelid);
		try {
			tooltip = $('#' + ddwObj.gridId).jqxGrid('getcellvalue', row, "DEFINITION");
		} catch (e) {
		}
		if (highlevelid == 'Y') {
			highlevelid = "<div><span id='span" + row + "' class='ui-icon ui-icon-plus'"
				+ " style='display:inline-block;cursor:pointer;' "
				+ "onclick=propertyHierarchy(" + row + ",'" + ddwObj.gridId + "','" + property.replace(/\s/g, "_") + "','PROPERTY_VALUE1')></span></div>";
		} else {
			highlevelid = "";
		}
		//alert('mand_ind::'+mand_ind);
		if (mand_ind == 'Y') {
			return "<div title='" + tooltip + "' style='width:100%' class='propMandatory'> <div style='width:90%'>" + property + "</div>" + highlevelid + "</div>";
		} else {
			return "<div title='" + tooltip + "' style='width:100%' class='propNormal'> <div style='width:100%'>" + property + highlevelid + "</div>";
		}
	};
	/*Renderer For Highlighting Mandatory Properties and Showing Multilevel Dr if applicable in Characteristic Tab */
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	for (var i = 0; i < datafields.length; i++) {
		if (typeof datafields[i].values != "undefined" && datafields[i].values != null) {

			var listboxData = eval(datafields[i].values.source);

			var dataFeildName = datafields[i].name;
			// var dataFeildNameId=dataFeildName+"_ID";
			if (dataFeildName.indexOf("_DLOV") > -1) {
				listTypeColNameId.push(dataFeildName);
			} else {
				listTypeColName.push(dataFeildName);
			}
			var listboxSource =
			{
				datatype: "json",
				datafields: [
					{ name: 'ListboxValue', type: 'string' },
					{ name: 'id', type: 'string' }
				],
				localdata: listboxData
			};
			var listBoxAdapter = new $.jqx.dataAdapter(listboxSource);
			datafields[i].values.source = listBoxAdapter.records;
			var changeFunObj = datafields[i].values;
			if (changeFunObj != null && changeFunObj['onchangeFunName'] != null && changeFunObj['onchangeFunName'] != '') {
			}
		}
	}
	var newLocalData = [];
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	var initDefaultFlag = $("#defaultFlag").val();
	var initattachType = $("#checkAttachType").val();
	if (!(initattachType != null && initattachType != '' && initattachType != undefined)) {
		if (gridInitParamObj != null) {
			var attachInitParams = gridInitParamObj["uuu_attachInitParams"];
			if (attachInitParams != null && attachInitParams != '' && attachInitParams != undefined) {
				var initParams = attachInitParams.split(":");
				if (initParams != null && initParams != '' && initParams != undefined) {
					$("#checkAttachType").val(initParams[1]);
				}
			}
		}
	}
	var source = $("#SOURCE").val();
	if (!(source != null && source != '' && source != undefined)) {
		if (gridInitParamObj != null) {
			var initParamSource = gridInitParamObj["uuu_Source"];
			$("#initParamSource").val(initParamSource);
		}
	}
	console.log("labelobj:::" + labelObject);
	if (localdata != null && localdata.length > 0 && listTypeColName.length > 0) {
		for (var i = 0; i < localdata.length; i++) {
			var dataObj = localdata[i];
			for (var j = 0; j < listTypeColName.length; j++) {
				dataObj[listTypeColNameId[j]] = dataObj[listTypeColName[j]];
				var displayKeyValuObj = dropDownListData[listTypeColName[j]];
				for (var k = 0; k < displayKeyValuObj.length > 0; k++) {
					var displayFieldObj = displayKeyValuObj[k];
					if (displayFieldObj != null && displayFieldObj != "" && displayFieldObj.id == dataObj[listTypeColName[j]]) {
						//console.log("listbox::::"+labelObject[displayFieldObj.ListboxValue]+"::::"+listTypeColName[j]);
						dataObj[listTypeColName[j]] = displayFieldObj.ListboxValue;

					}
				}
			}
			var defaultFlag = dataObj['DEFAULT_FLAG'];
			if (defaultFlag) {
				defaultFlag = "Y";
			} else {
				defaultFlag = "N";
			}
			var attachType = dataObj['ATTACH_TYPE'];
			if (attachType != null && attachType != '' &&
				initattachType != null && initattachType != '' && attachType == initattachType) {
				if (defaultFlag != null && defaultFlag != '' && initDefaultFlag != null
					&& initDefaultFlag != '' && initDefaultFlag == defaultFlag) {
					var imgSource = dataObj['CONTENT'];
					$("#descImage").attr("src", imgSource);
				}
			}
			newLocalData.push(dataObj);
		}
		if (newLocalData != null && newLocalData.length > 0) {
			localdata = [];
			localdata = newLocalData;
		}
	}
	//   console.log(JSON.stringify(localdata));
	var source =
	{
		datatype: "array",
		localdata: localdata,
		datafields: datafields
	};
	var dataAdapter = new $.jqx.dataAdapter(source);
	var isExportable = true;
	gridConfigObj = jsnobj.gridPropObj;

	for (var i = 0; i < columns.length; i++) {
		if (columns[i].cellsrenderer != null) {
			columns[i].cellsrenderer = eval('(' + columns[i].cellsrenderer + ')');
		}
		if (columns[i].createeditor != null) {
			columns[i].createeditor = eval('(' + columns[i].createeditor + ')');
		}
		if (columns[i].initeditor != null) {
			columns[i].initeditor = eval('(' + columns[i].initeditor + ')');
		}
		if (columns[i].geteditorvalue != null) {
			columns[i].geteditorvalue = eval('(' + columns[i].geteditorvalue + ')');
		}
		if (columns[i].cellbeginedit != null) {
			columns[i].cellbeginedit = eval('(' + columns[i].cellbeginedit + ')');
		}
		if (columns[i].rendered != null) {
			columns[i].rendered = eval('(' + columns[i].rendered + ')');
		}
	}
	gridConfigObj.source = dataAdapter;
	gridConfigObj.columns = columns;
	var paginationFlag = gridConfigObj['pageable'];
	if (paginationFlag) {
		gridConfigObj.virtualmode = false;
	}
	if (gridConfigObj['rowsheight'] != null && gridConfigObj['rowsheight'] != '') {//rowsheight
		gridConfigObj.autorowheight = true;
	}
	var renderToolbar = gridConfigObj.renderToolbar;
	gridConfigObj.renderToolbar = eval('(' + renderToolbar + ')');
	console.log("Editable::" + gridConfigObj.editable);
	console.log("tabId::::::6327::::" + tabId);
	try {
		$("#" + tabId).remove();
	} catch (e) {
	}
	if (erpDataFlag != 'Y') {

		$("#" + tabId + "_TABLE").after("<div id='" + tabId + "'></div>");
	} else {
		$("#" + tabId + "_FORM").after("<div id='" + tabId + "'></div>");
	}
	//    var pagerMode = $("#" + tabId).jqxGrid('pagermode');
	//    gridConfigObj.enabletooltips = false;
	gridConfigObj.cellhover = function(element, pageX, pageY) {
	};
	$("#" + tabId).jqxGrid(gridConfigObj);
	$('#' + tabId).jqxGrid('pagermode', 'simple');
	$('#' + tabId).on('celldoubleclick', function(event) {
		var args = event.args;
		var dataField = args.datafield;
		var dataField1 = args.text;
		var rowIndex = args.rowindex;
		var cellValue = args.value;
		var isEditable = $('#' + tabId).jqxGrid('getcolumnproperty', dataField, 'editable');
		console.log("isEditable::::" + isEditable)
		var editable = gridConfigObj.editable;
		if (!isEditable || !editable) {
			var column = $('#' + tabId).jqxGrid('getcolumn', event.args.datafield).text;
			popupedit(column, cellValue);
		}

	});
	var dataLength = source.localdata.length;
	try {
		if (dataLength <= 5) {
			$("#" + tabId).jqxGrid({ autoheight: true });
		}
	} catch (e) {
		console.log(e);
	}
	if (jsnobj.tbDdwEditFlag == true) {
		$("#" + tabId).jqxGrid('editable', false);
		$("#" + tabId).jqxGrid('selectionmode', 'multiple');
		//  gridConfigObj.editable = false;
	}
	var checkBoxFlag = false;
	$("#" + tabId).on('cellvaluechanged', function(event) {
		console.log("cell value changed");
		changeflag = true;
		if (checkBoxFlag) {
			checkBoxFlag = false;
			$("#" + tabId).jqxGrid('setcellvalue', event.args.rowindex, event.args.datafield, event.args.oldvalue);

		}
		var oldvalue = event.args.oldvalue;
		var newvalue = "";
		if (event.args.newvalue != null) {
			newvalue = event.args.newvalue.value
		}
		if (oldvalue != null && oldvalue != '' && oldvalue != undefined
			&& newvalue != null && newvalue != '' && newvalue != undefined && oldvalue == newvalue) {
			changeflag = false;
		}
	});
	var fieldVal;
	$("#" + tabId).on('cellbeginedit', function(event) {
		$("#" + tabId).attr('data-last-ed-field', event.args.datafield);
		$("#" + tabId).attr('data-last-ed-row', event.args.rowindex);
		// event arguments.
		var args = event.args;
		// column data field.
		var dataField = event.args.datafield;
		// row's bound index.
		var rowBoundIndex = event.args.rowindex;
		// cell value
		var value = args.value;
		cellOldValue = value;
		// cell old value.
		var oldvalue = args.oldvalue;

		// row's data.
		var rowData = args.row;
		var columntype = args.columntype;
		try {
			if (columntype == "dropdownlist") {
				fieldVal = rowData[dataField.replace("_DLOV", "")];
			}
		} catch (e) {
		}
		var columnType = event.args.columntype;
		if (columnInitParamsObj != null && columnInitParamsObj != '' && columnInitParamsObj != undefined) {
			var columnParams;
			if (columnType == 'dropdownlist') {
				columnParams = columnInitParamsObj[dataField.replace("_DLOV", "")];
			} else {
				columnParams = columnInitParamsObj[dataField];
			}
			if (columnParams != null && columnParams != '' && columnParams != undefined) {
				var editable = columnParams['uuu_editable'];
				if (editable != null && editable != '' && editable == "N") {
					var hiddenType = $('#' + tabId).jqxGrid('getcellvalue', rowBoundIndex, tabId + "_HIDDEN");
					if (hiddenType != null && hiddenType != '' && hiddenType != undefined && hiddenType != "INSERT") {
						$("#" + tabId).jqxGrid('endcelledit', rowBoundIndex, dataField, true);
						if (columnType == "checkbox") {
							checkBoxFlag = true;
						}
					}
				}
			}
		}
		$("#" + tabId).jqxGrid('selectrow', rowBoundIndex);
		$("#" + tabId + "_Update").show();
		//   //console.log("cell began event");
	});
	$("#" + tabId).bind('rowselect', function(event) {
		var selectedrowindexes = $("#" + tabId).jqxGrid('selectedrowindexes');
		var rwindex = event.args.rowindex;
		if (selecteIndexes.indexOf(rwindex) == -1) {
			selecteIndexes.push(rwindex)
		}
		var column = event.args.column;
		if (selecteIndexes.length != 0 && selectedrowindexes.length != 0) {
			$("#" + tabId + "_Delete").show();
			$("#" + tabId + "_Update").show();
		} else {
			$("#" + tabId + "_Delete").hide();
			$("#" + tabId + "_Update").hide();
		}
		if (selectedrowindexes.length == 0) {
			selecteIndexes.length = 0;
		}
		// ////console.log("PUSH:::::selecteIndexes.length:::" + selecteIndexes.length);
	});
	var onChangeFunctions = jsnobj.onChangeFunctions;
	$("#" + tabId).on('change', function(event) {
		var args = event.args;
		var currentTarget = event.currentTarget;
		var currentDataField = currentTarget.dataset.lastEdField;
		var currentRowIndex = currentTarget.dataset.lastEdRow;
		console.log("Select Changed ");
		if (args != null && args != '' && args.item != null && args.item != '' && fieldVal != args.item.label) {
			$("#" + tabId).jqxGrid('endcelledit', currentRowIndex, currentDataField, false);
		}
		if (onChangeFunctions != null) {
			var functionName = onChangeFunctions[currentDataField];
			if (functionName != null) {
				functionName = functionName.replace("'rowIndex'", currentRowIndex);
				eval(functionName);
			}
		}
	});
	$("#" + tabId).bind('rowunselect', function(event) {
		var selectedrowindexes = $("#" + tabId).jqxGrid('selectedrowindexes');
		// ////console.log("rowunselect:::::"+selectedrowindexes);
		var rwindex = event.args.rowindex;
		selecteIndexes.pop(rwindex)
		if (selecteIndexes.length != 0 && selectedrowindexes.length != 0) {
			$("#" + tabId + "_Delete").show();
			$("#" + tabId + "_Update").show();
		} else {
			$("#" + tabId + "_Delete").hide();
			$("#" + tabId + "_Update").hide();
		}
		if (selectedrowindexes.length == 0) {
			selecteIndexes.length = 0;
		}
	});
	$("#" + tabId).on('rowclick', function(event) {
		$("#" + tabId + '_Update').show();
		$("#" + tabId + '_Delete').show();
	});
	$("#" + tabId + "_MO_COUNT").text("");
	$("#" + tabId + "_ICON").hide();
}// end of formGrid()
function refreshErpTab(selectedErpGridData, erpTabGridId) {
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {

	}
	tabsOldData = {};
	alert(":::::" + JSON.stringify(selectedErpGridData));
	console.log(":::::::::" + JSON.stringify(selectedErpGridData));
	var editableFlag = "N";
	if (selectedErpGridData != null) {
		var basicData = {};
		$("#mat_creation_form_table :input").each(function() {
			var textid = $(this).attr("id");
			var type = $(this).attr("type");
			var textval = $(this).val();
			if (type != 'hidden') {
				if (textval != null && textval != '') {
					textval = textval.toUpperCase();
				}
			}
			//                  jsonOBJ.ids.push(textid.toLowerCase());
			if (textid != null && textid != 'CREATE_DATE') {
				if (selectedErpGridData[textid] != null) {
					textval = selectedErpGridData[textid];
				}
				basicData[textid] = textval;
			}

			if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
				var columnNames = $("#" + textid).val();
				var columnsArray = columnNames.split(",");

				var hiddenIds = textid.split("HIDDEN_");
				var hiddenVal = $("#" + hiddenIds[1]).val();
				if (selectedErpGridData[hiddenIds[1]] != null) {
					hiddenVal = selectedErpGridData[hiddenIds[1]];
				}
				for (var i = 0; i < columnsArray.length; i++) {
					basicData[columnsArray[i]] = hiddenVal;
				}

			}


		});
		var instance = $("#INSTANCE").val();
		var plant = $("#BUSINESS_UNIT").val();
		console.log(basicData['INSTANCE'] + ":::instance::::" + instance);
		console.log(basicData['BUSINESS_UNIT'] + ":::BUSINESS_UNIT::::" + plant);
		if (basicData != null && basicData['INSTANCE'] == instance && plant == basicData['BUSINESS_UNIT']) {

			editableFlag = "Y";

		} else {
			editableFlag = "N"
			delete basicData['SOURCE'];
		}

		basicData['editableFlag'] = editableFlag;
		basicData['erpTabGridId'] = erpTabGridId;
		//        alert("::basicData:::" + JSON.stringify(basicData));
		var jsonOBJ = {};
		jsonOBJ.basicData = basicData;
		// var erpTabGridId =  $("#erpTabGridId").val();
		console.log("JSON.stringify(basicData):::" + JSON.stringify(basicData))
		$.ajax({
			type: "POST",
			url: 'selectRecord',
			data: {
				'jsonData': JSON.stringify(jsonOBJ),
				'panelId': $("#panelId").val(),
				'gridId': erpTabGridId
				//                'gridId': $("#erpTabGridId").val()
			},
			//headers: {"Access-Control-Allow-Origin": true},
			traditional: true, cache: false,
			success: function(response) {
				//  alert(response);
				var jsnobj = JSON.parse(response);
				if (jsnobj != null) {
					var erpDataObj = jsnobj;
					$('#' + erpTabGridId).jqxTabs('destroy');
					$('#' + erpTabGridId + "_TABLE").after('<div id="' + erpTabGridId + '"></div>');

					$('#' + erpTabGridId).html(erpDataObj['tabString']);
					// $('#erpData').html(erpDataObj['tabString']);

					$('#' + erpTabGridId).jqxTabs({ position: 'top', width: '100%', reorder: true, theme: 'ui-redmond', keyboardNavigation: true });
					$('#' + erpDataObj['tabGridId'] + "_FORM").html(erpDataObj['data']);

					if (erpDataObj['erpTabGridId'] != null && erpDataObj['erpTabGridId'] != '' && (erpDataObj['erpTabGridId']).toString().indexOf("_OLD") == -1) {
						//  $("#erpTabGridId").val(erpDataObj['erpTabGridId']);
						$("#erpTabGridId").val(erpDataObj['erpTabGridId']);
					}
					//                    $("#mat_creation_form_table").append("<input type='hidden' id='" + erpDataObj['erpTabGridId'] + "HiddenGridData' value='' />");
					$("#" + erpDataObj['erpTabGridId'] + "HiddenGridData").val(erpDataObj['gridIds']);
					if (editableFlag == 'Y') {
						$('#' + erpDataObj['tabGridId'] + "Icon").html(erpDataObj['tabOperationIcon']);
						$("#" + erpDataObj['tabGridId'] + " :input[data-type='D']").each(function() {
							var id = $(this).attr('id');
							//                                    var id = "#" + id;
							//                                    //////////alert(id);
							var isEditable = $("#" + id).attr('data-editable');
							if (isEditable == "Y") {
								$("#" + id).datepicker({
									changeMonth: true,
									changeYear: true,
									dateFormat: "dd-mm-yy",
									showOn: "button",
									buttonImage: 'images/date_picker_icon.png',
									buttonImageOnly: true
								});
							}
						});
						var tabOldObj = {};
						$("#" + erpDataObj['tabGridId'] + "_TABLE" + " :input").each(function() {
							var textid = $(this).attr("id");
							var type = $(this).attr("type");
							var textval = $(this).val();
							if (type != 'hidden') {
								if (textval != null && textval != '') {
									textval = textval.toUpperCase();
								}
							}
							if (type != null && type == 'checkbox') {//
								if ($("#" + textid).is(':checked')) {
									textval = "Y";
								} else {
									textval = "N";
								}
							}
							//                  jsonOBJ.ids.push(textid.toLowerCase());
							if (textid != null && textid != 'CREATE_DATE') {
								tabOldObj[textid] = textval;
							}
						});
						if (tabOldObj != null) {
							tabsOldData[erpDataObj['tabGridId']] = tabOldObj;
						}
					}
					$("#erpTabGridId").val(erpDataObj['erpTabGridId']);
					$("#" + erpDataObj['erpTabGridId'] + "HiddenGridData").val(erpDataObj['gridIds']);
					var UnselectedGridId;

					var matchedcount = 0;
					$('#' + erpTabGridId).on('unselecting', function(event) {
						UnselectedGridId = globalErpTab;

						console.log("before matchedcount : " + matchedcount);
						console.log("change matchedcount : " + changeflag);
						//console.log("UnselectedGridId : "+UnselectedGridId);

						var jsonOBJ = {};
						jsonOBJ.feildIds = [];
						jsonOBJ.feildValues = [];
						matchedcount = 0;
						console.log("after matchedcount : " + matchedcount);

						var dataView = $("#" + UnselectedGridId + "_Update").attr("data-view");
						var selectedTabOldData = tabsOldData[UnselectedGridId];
						if (dataView == "FORM-VIEW" && editableFlag == 'Y') {
							changeflag = false;
							$("[id*=" + UnselectedGridId + "]  :input").each(function() {
								var textid = $(this).attr("id");
								var type = $(this).attr("type");
								var textval = $(this).val();
								//                console.log("textid:::" + textid);
								if (type != 'hidden') {
									if (textval != null && textval != '') {
										textval = textval.toUpperCase();
									}
								}
								if (type != null && type == 'checkbox') {//
									if ($("#" + textid).is(':checked')) {
										textval = "Y";
									} else {
										textval = "N";
									}
								}
								var textOldVal = "";
								if (selectedTabOldData != null) {
									textOldVal = selectedTabOldData[textid];
									if (textid != null && textid != 'CREATE_DATE' && textval != textOldVal) {
										matchedcount++;
									}
								}
								if (matchedcount > 0) {
									changeflag = true;
								}
							});
						}
						if (dataView == "GRID-VIEW") {
							matchedcount = 1;
							console.log(" GRID-VIEW CODE");
							var changecount = 0;
							$("[id^=contenttable]  :input").each(function() {
								//            var textid = $(this).attr("id");
								var type = $(this).attr("type");
								var textval = $(this).val();
								console.log(" type : " + type + " textval : " + textval + " cell old value " + cellOldValue);

								if (type == 'textbox' && textval != null && textval != cellOldValue) {
									changecount++;
								}
							});
							if (changecount > 0) {
								console.log(" grid input changed ccount " + changecount);
								changeflag = true;
								console.log("grid input changed : changeflag = " + changeflag);
							}
						}
					});

					$('#' + erpTabGridId).on('selecting', function(event) {
						var gridIds = $("#" + erpTabGridId + "HiddenGridData").val();
						//                        var gridIds = $("#erpDataGridId").val();
						var gridIdsArry = gridIds.split(",");
						var selectedGridId = gridIdsArry[event.args.item];

						if (changeflag && matchedcount > 0) {
							if (tabSwitchflag) {
								event.cancel = true;
								// event.preventDefault();
							}
							$("#logoutDailog").html((labelObject['Unsaved changes will be lost'] != null ? labelObject['Unsaved changes will be lost'] : 'Unsaved changes will be lost') + ", " + (labelObject['you like to save'] != null ? labelObject['you like to save'] : 'you like to save') + "?");
							$("#logoutDailog").dialog({
								title: (labelObject['Error'] != null ? labelObject['Error'] : 'Error'),
								modal: true,
								width: 300,
								height: 135,
								fluid: true,
								buttons: [
									{
										text: (labelObject['Yes'] != null ? labelObject['Yes'] : 'Yes'),
										click: function() {
											$(this).html("");
											$(this).dialog("close");
											$(this).dialog("destroy");
										}
									}, {
										text: (labelObject['No'] != null ? labelObject['No'] : 'No'),
										click: function() {
											$(this).html("");
											$(this).dialog("close");
											tabSwitchflag = false;
											var selectedTab = event.args.item;
											console.log("selectedTab  :: " + selectedTab);
											changeflag = false;
											$('#' + erpTabGridId).jqxTabs('select', selectedTab);
											fetchErpTab(selectedGridId, erpTabGridId);
											$(this).dialog("close");

											changeflag = false;
											tabSwitchflag = true;

											console.log(length + " tabSwitchflag  : " + tabSwitchflag);

										}
									}
								],
								open: function() {
									$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
									$(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
									$(".visionHeaderMain").css("z-index", "999");
									$(".visionFooterMain").css("z-index", "999");
								},
								beforeClose: function(event, ui) {
									$(".visionHeaderMain").css("z-index", "99999");
									$(".visionFooterMain").css("z-index", "99999");
								}
							});
						}

						if (!changeflag) {
							fetchErpTab(selectedGridId, erpTabGridId);
						} else if (erpTabGridId != null && erpTabGridId.indexOf("_OLD") > 0) {
							fetchErpTab(selectedGridId, erpTabGridId);
						}
						// fetchErpTab(selectedGridId, erpTabGridId);
					});
				}

			},
			error: function(e) {
				//  (e.message)
				sessionTimeout(e);
			}
		});
	}
}
function fetchAttachmentsTabGridData(tabId, tabOldId, dependentAccorId, currntAccorId, wrapInd) {
	ajaxStart();
	console.log("fetchAttachmentsTabGridData ::: START");
	var dependentAccorId = dependentAccorId;
	if (currntAccorId > -1) {
		alert(currntAccorId);
		// startAjax();
		$("[class*=_OLD]").addClass("ui-state-disabled");
		$(".ui-state-disabled").not(dependentAccorId).next("div").hide();
		$(dependentAccorId).next("div").toggle();
		$(".visionAccordionSeperator").remove();
		$("#" + tabId).after("<div class='visionAccordionSeperator'></div>");
		fetchAttachmentsTabGridData(tabOldId, tabId, '', '-1', 1);
	}
	var basicData = {};
	$("#mat_creation_form_table :input").each(function() {
		var textid = $(this).attr("id");
		var type = $(this).attr("type");
		var textval = $(this).val();
		if (type != 'hidden') {
			if (textval != null && textval != '') {
				textval = textval.toUpperCase();
			}
		}
		if (textid != null && textid != 'CREATE_DATE') {
			basicData[textid] = textval;
		}
		if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
			var columnNames = $("#" + textid).val();
			var columnsArray = columnNames.split(",");
			var hiddenIds = textid.split("HIDDEN_");
			var hiddenVal = $("#" + hiddenIds[1]).val();
			for (var i = 0; i < columnsArray.length; i++) {
				if (hiddenVal != null) {
					hiddenVal = hiddenVal.toUpperCase();
				}
				basicData[columnsArray[i]] = hiddenVal;
			}
		}
	});
	if (!fetchattach) {
		$.ajax({
			type: "post",
			traditional: true,
			url: "fetchAttachmentTab",
			data: {
				basicData: JSON.stringify(basicData),
				gridId: tabId
			},
			cache: false,
			async: true,
			dataType: 'json',
			success: function(result) {
				ajaxStop();
				if (result != null && result != undefined) {
					var gridObj = result['gridObj'];
					gridObj['data'] = result['attachmentArray'];
					gridObj['panelData'] = result['panelObject'];
					formGrid(tabId, gridObj, 'N');
				}
			},
			error: function(e) {
				//  alert(e.message)
				sessionTimeout(e);
			}
		});
	}
	console.log("fetchAttachmentsTabGridData ::: END ");
	changeflag = false;
}
function changeflagFuction() {
	changeflag = false;
	console.log("changeflagFuction -> " + changeflag);
}
//function eval(x) { 
//};
var fetchattach = false;
function fetchAttachmentsTabData(tabId, tabOldId, dependentAccorId, currntAccorId, wrapInd) {
	console.log("fetchAttachmentsTabData ::: START");
	var dependentAccorId = dependentAccorId;
	if (currntAccorId > -1) {
		alert(currntAccorId);
		// startAjax();
		$("[class*=_OLD]").addClass("ui-state-disabled");
		$(".ui-state-disabled").not(dependentAccorId).next("div").hide();
		$(dependentAccorId).next("div").toggle();
		$(".visionAccordionSeperator").remove();
		$("#" + tabId).after("<div class='visionAccordionSeperator'></div>");
		fetchAttachmentsTabData(tabOldId, tabId, '', '-1', 1);
	}


	var record_No = $('#RECORD_NO').val();
	var specModelNo = $("#SPEC_MODEL_NO").val();


	var requestNumber = $('#REQ_NUMBER').val();
	var baskettype = $('#baskettypehid').val();
	var enclosureedit = $("#encEditable").val();
	if (!fetchattach) {
		ajaxStartAttachments();
		setTimeout(function() {
			$.ajax({
				type: "post",
				traditional: true,
				// url: "SelectFiles?recordNo=" + record_No + "&&specModelNo= " + specModelNo + "&&baskettype=" + baskettype + "&&reqNumber=" + requestNumber + "&&tabId=" + tabId + "&&enclosureEdit=" + enclosureedit,
				url: "SelectFiles?recordNo=" + record_No + "&&specModelNo= " + specModelNo + "&&baskettype=" + baskettype + "&&reqNumber=" + requestNumber + "&&tabId=" + tabId + "&&enclosureEdit=" + enclosureedit,
				//url: "SelectFiles?recordNo=" + record_No + "&&baskettype=" + baskettype + "&&reqNumber=" +requestNumber +" &&tabId=" +tabId,
				cache: false,
				async: false,
				dataType: 'html',
				success: function(result) {
					$("#" + tabId).html(result);
					$("#" + tabId).removeClass('visionEnclosureTable');
					$("#" + tabId).addClass('visionEnclosureTable');
					var i = 0;
					if ($("#baskettypehid").val() == 'Search_View'
						||
						$("#baskettypehid").val() == '_New_Extension_Requests' ||
						$("#baskettypehid").val() == '_New_Change_Requests' ||
						$("#baskettypehid").val() == '_New_Deletion_Requests' ||
						$("#baskettypehid").val() == '_New_Undeletion_Requests') {

					}
					fetchattach = false;
				},
				error: function(e) {
					//  alert(e.message)
					sessionTimeout(e);
				}

			});
		}, 888);
	}
}
function insertGridRow(tabId, dataView, viewId) {
	//function insertGridRow(tabId, dataView, datafields, viewId, localData) {
	//console.log("insertGridRow:::"+tabId);
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {

	}
	//console.log(":::datafields::" + JSON.stringify(datafields)  );
	if (dataView == "GRID-VIEW") {
		// //////console.log("k");
		var sourceex = $("#" + tabId).jqxGrid('source');
		///code for defaultValues values
		var defaultValuesArray = [];
		var defaultValues = $("#" + tabId + "_defaultValues").val();
		if (defaultValues != null && defaultValues != '') {
		} else {
			defaultValues = $("#defaultValues").val();
		}

		var finalvalue = [];
		var colvalue = [];
		var colnameArray = [];
		var colvalueArray = [];

		//        ////////////////alert("defaultValues******" + defaultValues);
		if (defaultValues != null) {
			defaultValuesArray = defaultValues.split(",");

			for (var i = 0; i < defaultValuesArray.length; i++) {
				finalvalue = defaultValuesArray[i];
				colvalue = finalvalue.split(":");
				colnameArray.push(colvalue[0]);
				//                colvalueArray.push(colvalue[1]);

				if (colvalue[0] == "QSREC") {

					var reciepientType = "OT";
					var panCharTop;
					panCharTop = $("#PAN_NUMBER").val();
					if (panCharTop && panCharTop.charAt(3) == "C") {
						reciepientType = "CO";
					}
					colvalueArray.push(reciepientType);
				}
				//                else if (colvalue[0] == "WH_APPLICABALITY" || colvalue[0] == "LIABLE")
				//                {
				//                    colvalueArray.push("YES");
				//                }
				else if (colvalue[0] == "WH_APPLICABALITY"
					|| colvalue[0] == "LIABLE"
					|| colvalue[0] == "TDS_APPL"
					|| colvalue[0] == "WT_AGENT_TAX"

					// || colvalue[0] == "WH_APPLICABALITY_DLOV"
					//|| colvalue[0] == "LIABLE_DLOV"
				) {
					//colvalue[1]=colvalue[1] == 'NO' ? "N" : "Y";
					//                    colvalue[1]=colvalue[1] == 'N' ? "NO" : "YES";

					if (colvalue[1] == 'N') {
						if (colvalue[0] == "WH_APPLICABALITY" || colvalue[0] == "TDS_APPL" || colvalue[0] == "WT_AGENT_TAX") {
							colvalue[1] = 'YES';
						} else {
							colvalue[1] = 'NO';
						}

					} else if (colvalue[1] = colvalue[1] == 'Y') {
						colvalue[1] = 'YES';
					}

					colvalueArray.push(colvalue[1]);
				} else {
					if (colvalue[0] != null && colvalue[0] != '' && colvalue[0].indexOf("_DLOV") == -1 && colvalue[1] == 'Y') {
						colvalue[1] = true;
					}
					colvalueArray.push(colvalue[1]);
				}

			}


		}

		//////end 

		var dataFeilds = [];
		dataFeilds = sourceex._source.datafields;

		var row = {};

		for (var key in dataFeilds) {
			var value = dataFeilds[key];
			if (value != null && value['name'] != null && value['name'] != '') {
				var columnName = value['name'];
				console.log("==================columnName::::" + columnName + ":::::::viewId:::" + viewId + "::tabId::::::" + tabId);
				if (columnName != null && columnName != '' && columnName == viewId + "_HIDDEN") {
					row[columnName] = "INSERT";
				} else if (columnName != null && columnName != '' && columnName == 'BKONT') {
					//                row[columnName] = "01";
				} else if (columnName != null && columnName != '' && jQuery.inArray(columnName, colnameArray) > -1) {
					for (var cv = 0; cv <= colnameArray.length; cv++) {
						if (columnName == colnameArray[cv])
							row[columnName] = colvalueArray[cv];
					}

				} else if (value.type != null && value.type != "" && (value.type === "boolean" || value.type === "bool")) {
					row[columnName] = false;
					// row['cellclassname'] = "vendorno_style2";
				} else {
					row[columnName] = "";
					// row['cellclassname'] = "vendorno_style2";
				}

			}
		}

		console.log("row:::" + JSON.stringify(row));
		//        var localdata = JSON.stringify(localData).length;
		var tabOperationFlag = false;
		var data = $("#" + tabId).jqxGrid('getrowdata', 0);
		console.log("data:::" + JSON.stringify(data));
		alert("tab id is " + tabId);
		if (data == null) {
			alert("data is null");
			$("#" + tabId).jqxGrid({ showfilterrow: false });
			try {
				//                $("#" + tabId).jqxGrid('clearfilters');
				//                $("#" + tabId).jqxGrid('clearselection');
			} catch (e) {
			}
			var index = $("#" + tabId).jqxGrid('getrowboundindex', 1);
			index = index + 1;
			console.log("index IF ::::::" + index);
			$("#" + tabId).jqxGrid('selectrow', index);
			tabOperationFlag = true;
		} else {
			alert("data is not null");
			// //////console.log("no:::");
			$("#" + tabId).jqxGrid({ showfilterrow: false });
			try {
				//                $("#" + tabId).jqxGrid('clearfilters');
				//                $("#" + tabId).jqxGrid('clearselection');
			} catch (e) {
			}
			var index = $("#" + tabId).jqxGrid('getrowboundindex', 0);
			index = index;
			console.log("index else ::::::" + index);
			$("#" + tabId).jqxGrid('selectrow', index);
			var tabHidden = viewId + "_HIDDEN";
			if (data[tabHidden] != null && data[tabHidden] != '' && data[tabHidden] != 'INSERT') {
				tabOperationFlag = true;
			}

		}
		console.log("tabOperationFlag:::" + tabOperationFlag);

		if (tabOperationFlag) {
			var objectKeys = Object.keys(row);
			var objectKeysArray = objectKeys.toString().split(",");
			var lovkey = "";
			for (var i = 0; i < objectKeysArray.length; i++) {
				//                if (row[objectKeysArray[i]] != null && row[objectKeysArray[i]].toUppercase() == 'AN-AN-NAN') {
				//                    row[objectKeysArray[i]] = "";
				//                }
				if (objectKeysArray[i].indexOf("_DLOV") > -1) {
					lovkey = objectKeysArray[i].replace("_DLOV", "");
					if (row[lovkey] == 'YES') {
						row[objectKeysArray[i]] = "Y";
					} else if (row[lovkey] == 'NO') {
						row[objectKeysArray[i]] = "N";
					}
				}
			}


			var commit = $("#" + tabId).jqxGrid('addrow', null, row, 0);
			$("#" + tabId).jqxGrid('selectrow', 0);
		}

		$('#' + viewId + '_Add').css("display", "none");

	}
}
function fetchErpTab(selectedGridId, erpTabGridId) {
	labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	tabsOldData = {};
	globalErpTab = selectedGridId;
	alert("erpTabGridId::::" + erpTabGridId);
	console.log("erpTabGridId:::" + erpTabGridId);
	var selectedErpGridData = null;
	if (erpTabGridId == '') {// || erpTabGridId == 'undefined'
		erpTabGridId = $("#erpTabGridId").val();
	}
	try {
		selectedErpGridData = $('#' + erpTabGridId + "_TABLE").jqxGrid('getrowdata', $('#' + erpTabGridId + "_TABLE").jqxGrid('getselectedrowindex'));
	} catch (e) {
	}
	alert(erpTabGridId + ":::fetchErpTabData::::" + selectedGridId);
	alert("selectedErpGridData::::" + JSON.stringify(selectedErpGridData));
	var editableFlag = "N";
	var basicData = {};
	$("#mat_creation_form_table :input").each(function() {
		var textid = $(this).attr("id");
		var type = $(this).attr("type");
		var textval = $(this).val();
		if (type != 'hidden') {
			if (textval != null && textval != '') {
				textval = textval.toUpperCase();
			}
		}
		if (textid != null && textid != 'CREATE_DATE') {
			if (selectedErpGridData != null && selectedErpGridData[textid] != null) {
				textval = selectedErpGridData[textid];
			}
			basicData[textid] = textval;
		}

		if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
			var columnNames = $("#" + textid).val();
			var columnsArray = columnNames.split(",");

			var hiddenIds = textid.split("HIDDEN_");
			var hiddenVal = $("#" + hiddenIds[1]).val();
			if (selectedErpGridData != null && selectedErpGridData[hiddenIds[1]] != null) {
				hiddenVal = selectedErpGridData[hiddenIds[1]];
			}
			for (var i = 0; i < columnsArray.length; i++) {
				basicData[columnsArray[i]] = hiddenVal;
			}
		}
	});
	alert("basicData::::" + JSON.stringify(basicData));
	var instance = $("#INSTANCE").val();
	var plant = $("#BUSINESS_UNIT").val();
	alert(basicData['INSTANCE'] + "::::instance:::" + instance);
	alert(basicData['BUSINESS_UNIT'] + "::::plant:::" + plant);
	var rolehid = $("#rolehid").val();
	if (rolehid != null && (rolehid.indexOf("VM_") > -1 || rolehid.indexOf("CM_") > -1)) {
		editableFlag = "Y";
	} else {
		if (basicData != null && basicData['INSTANCE'] == instance && plant == basicData['BUSINESS_UNIT']) {
			if (erpTabGridId != null && erpTabGridId.indexOf("_OLD") > -1) {
				editableFlag = "N";
			} else {
				editableFlag = "Y";
			}
		}
	}
	basicData['editableFlag'] = editableFlag;
	basicData['erpTabGridId'] = erpTabGridId;
	if (selectedGridId != null) {
		$.ajax({
			type: "POST",
			url: 'fetchERPTabaData',
			data: {
				'gridId': selectedGridId,
				'basicData': JSON.stringify(basicData),
				'panelId': $("#panelId").val(),
				'erpTabGridId': erpTabGridId
			},
			//headers: {"Access-Control-Allow-Origin": true},
			traditional: true, cache: false,
			success: function(response) {
				ajaxStop();//26
				alert("response::::" + response);
				var erpDataObj = JSON.parse(response);
				$('#' + selectedGridId + "Icon").html(erpDataObj['tabOperationIcon']);
				if (erpDataObj['intiobj'] != null) {
					var materialTyp = $("#RECORD_TYPE").val();
					//uuu_CalButtonType
					var initObj = erpDataObj['intiobj'];
					var calButtonType = initObj['uuu_CalButtonType'];

					console.log("calButtonType:::" + calButtonType);
					// console.log("calculateindex::::"+ calButtonType.indexOf(materialTyp));
					if (calButtonType == null || calButtonType == '') {
						calButtonType = 'ALL';
					}
					console.log("calButtonType2:::" + calButtonType);
					if ((calButtonType == 'ALL' || calButtonType.indexOf(materialTyp) > -1)) {

						$('#' + selectedGridId + '_CalculateStock').show();
					} else {
						$('#' + selectedGridId + '_CalculateStock').hide();
					}
				}
				console.log(erpDataObj['dataLength']);
				//  if (erpDataObj['formView'] == "GRID-VIEW") {
				var formDefaultValues = "";
				formDefaultValues = erpDataObj['defaultValues'];

				$("#defaultValues").val(formDefaultValues);
				$("#" + selectedGridId + "_defaultValues").remove();
				$("#mat_creation_form_table").append("<input type='hidden' id='" + selectedGridId + "_defaultValues' />");
				$("#" + selectedGridId + "_defaultValues").val(formDefaultValues);
				if (erpDataObj['dataLength'] != 1) {

					console.log("::::::" + JSON.parse(erpDataObj['data']));
					// var jsonObj  = JSON.parse(erpDataObj['data']);tabOperationIcon
					$("#" + selectedGridId + "Icon").hide();
					//                    $("#" + selectedGridId ).show();
					formGrid(selectedGridId, JSON.parse(erpDataObj['data']), erpDataObj['erpData']);

				} else {
					try {
						$("#" + selectedGridId).jqxGrid('destroy');
					} catch (e) {

					}

					$("#" + selectedGridId + "_FORM").html(erpDataObj['data']);
					if (editableFlag != null && editableFlag == 'Y') {
						$('#' + selectedGridId + "Icon").show();
						$("#" + selectedGridId + "_FORM" + " :input[data-type='D']").each(function() {
							var id = $(this).attr('id');

							var isEditable = $("#" + id).attr('data-editable');
							if (isEditable == "Y") {
								$("#" + id).datepicker({
									changeMonth: true,
									changeYear: true,
									dateFormat: "dd-mm-yy",
									showOn: "button",
									buttonImage: 'images/date_picker_icon.png',
									buttonImageOnly: true
								});
							}
						});
						var tabOldObj = {};

						$("#" + selectedGridId + "_TABLE" + " :input").each(function() {
							var textid = $(this).attr("id");
							var type = $(this).attr("type");
							var textval = $(this).val();
							if (type != 'hidden') {
								if (textval != null && textval != '') {
									textval = textval.toUpperCase();
								}
							}
							if (type != null && type == 'checkbox') {//
								if ($("#" + textid).is(':checked')) {
									textval = "Y";
								} else {
									textval = "N";
								}
							}
							//                  jsonOBJ.ids.push(textid.toLowerCase());
							if (textid != null && textid != 'CREATE_DATE') {
								tabOldObj[textid] = textval;
							}
						});
						if (tabOldObj != null) {
							tabsOldData[selectedGridId] = tabOldObj;
						}
					} else {
						$("#" + selectedGridId + "Icon").hide();
					}
				}
			},
			error: function(e) {
				//  (e.message)
				sessionTimeout(e);
			}
		});
	}
}
var colorcount = 0;
function getDictionaryPropertyDetails(classConceptId, className, propertyConceptId, property, event) {
	showLoader();
	var outerText = event.target.outerText;
	var offsetParent = event.target.offsetParent.className;
	var dictbl = $("#dictionarytbl").find("td");
	var domain = $('#SelectedValue').val();

	$.ajax({
		type: "POST",
		url: 'getDictionaryPropertyDetails',
		data: {
			'property': property,
			'className': className,
			'classConceptId': classConceptId,
			'propertyConceptId': propertyConceptId,
			domain: domain,
		},
		traditional: true,
		cache: false,
		success: function(result) {
			stopLoader();
			$("#searchresultsSplitter").html(result);
			$("#searchresultsSplitter").show();
			if (dictbl.hasClass('coloredActive') && colorcount >= 1) {
				dictbl.removeClass("coloredActive");
			}
			if (outerText != null && outerText != '' && outerText != undefined && outerText == property) {
				$("." + offsetParent).addClass("coloredActive");
				colorcount++;
				//                $("."+oldClickedValue).removeClass("coloredActive");
			}
		}

	});
}
function showBrowseButton(param, tabId, dataView) {
	$(".addIcon_" + param).hide();
	if (tabId == "MM_ATTACHMENTS_OLD") {
		var listval1 = $('#listOld_' + param).val();
	} else {
		var listval1 = $('#list_' + param).val();
	}

	var encvalue = listval1;
	var record_No = $('#RECORD_NO').val();
	var specModelNo = $("#SPEC_MODEL_NO").val();
	var vendorId = $('#VENDOR_ID').val();
	var baskettype1 = $("#baskettypehid").val();
	var source = $('#SOURCE').val();
	var accountId = $('#ACCOUNT_ID').val();
	if (!(source != null && source != '' && source != undefined)) {
		source = $("#" + tabId).attr("initParamSource");
		if (!(source != null && source != '' && source != undefined)) {
			source = $("#initParamSource").val();
		}
	}
	var defaultFlag = "";
	var baskettype = "";
	if (baskettype1 != null && baskettype1 != '') {
		baskettype = baskettype1.replace(/\s/gi, "_");
	}
	var request_number = $("#REQ_NUMBER").val();
	var locate_code = $("#locatcode").val();
	var url = 'UploadAttachFiles';
	var params = {
		tabId: tabId,
		gridId: tabId
	};
	if (dataView != null && dataView != '' && dataView == "GRID-VIEW") {
		params['panelId'] = $("#panelId").val();
		params['tableName'] = $("#tableName").val();
		listval1 = $('#' + tabId).jqxGrid('getcellvalue', 0, "ATTACH_TYPE");
		var checkAttachType = $("#checkAttachType").val();
		if (!(checkAttachType != null && checkAttachType != '' && checkAttachType != undefined)) {
			checkAttachType = $("#" + tabId).attr("checkAttachType");
		}
		params['checkAttachType'] = checkAttachType;
		defaultFlag = $('#' + tabId).jqxGrid('getcellvalue', 0, "DEFAULT_FLAG");
		if (defaultFlag) {
			defaultFlag = "Y";
		} else {
			defaultFlag = "N";
		}
		params['defaultFlag'] = defaultFlag;
		params['source'] = source;
		params['accountId'] = accountId;
		var masterId = $("#mastergridid").val();
		if (masterId != null) {
			var selectedrowindex = $('#' + masterId).jqxGrid('getselectedrowindex');
			record_No = $('#' + masterId).jqxGrid('getcellvalue', selectedrowindex, "RECORD_NO");
			request_number = $('#' + masterId).jqxGrid('getcellvalue', selectedrowindex, "REQ_NUMBER");
			specModelNo = $('#' + masterId).jqxGrid('getcellvalue', selectedrowindex, "SPEC_MODEL_NO");
			var spirRecId = $('#' + masterId).jqxGrid('getcellvalue', selectedrowindex, "SPIR_REC_ID");
			vendorId = $('#' + masterId).jqxGrid('getcellvalue', selectedrowindex, "VENDOR_ID");
			params['spirRecId'] = spirRecId;
		}
	}
	params['recordNo'] = record_No;
	params['requestNumber'] = request_number;
	params['specModelNo'] = specModelNo;
	params['attachType'] = listval1;
	params['vendorId'] = vendorId;
	window.chckValues.push(param);
	var attach_val = "Y";
	if (attach_val.trim() == 'Y') {
		var id = "#browseTdId_" + param;
		if (tabId == "MM_ATTACHMENTS_OLD") {
			browseId = "#browseIdOld_" + param;
		} else {
			browseId = "#browseId_" + param;
		}
		if (dataView == "GRID-VIEW") {
			browseId = "#visionColFileId";
		}
		$(id).show();
		var validExtensions = ['xps', 'gif', 'png', 'jpg', 'jpeg', 'tif', 'tiff', 'pdf', 'bmp', 'xls', 'xlsx', 'doc', 'docx', 'txt'];
		var validExtensionsString = $(browseId).attr("accept");
		if (validExtensionsString != null && validExtensionsString != 'undefined' && validExtensionsString != '') {
			var validExtensionsStr = validExtensionsString.replace(/\./gi, "");
			validExtensions = validExtensionsStr.split(',');
		}
		$(browseId).ajaxfileupload({
			'action': url,
			params: params,
			valid_extensions: validExtensions,
			'onComplete': function(response) {

				$("#wait").css("display", "none");

				var serverResponce = JSON.stringify(response.message);

				$(id).hide();

				if (serverResponce.lastIndexOf("File is Empty,Can't be Uploaded.") > -1) {//File is Empty,Cann't be Uploaded.

					var baskettype = $("#baskettypehid").val();
					var modalObj = {
						title: 'Message',
						body: serverResponce,
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {

							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("dataDxpSplitterValue", modalObj);
				} else if (serverResponce.lastIndexOf("Size of each file should not exceed 5000KB.") > -1) {//Size of each file should not exceed 5000KB.

					var modalObj = {
						title: 'Message',
						body: serverResponce,
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {

							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("dataDxpSplitterValue", modalObj);

				} else if (serverResponce.lastIndexOf("Maximum Size") > -1) {//Size of each file should not exceed based on max size in battachtype table.
					var res = serverResponce.replace('"', '').replace('"', '').replace('"', '').replace('"', '').replace('"', '').replace('"', '');
					var modalObj = {
						title: 'Message',
						body: res,
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {

							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("dataDxpSplitterValue", modalObj);

				} else if (serverResponce.lastIndexOf("Please Upload Image or PDF File Only.") > -1
					|| serverResponce.lastIndexOf("Please Upload files with") > -1) {//Size of each file should not exceed 5000KB.
					var modalObj = {
						title: 'Message',
						body: "Please Upload files with " + validExtensionsString + " extension(s) only",
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {

							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("dataDxpSplitterValue", modalObj);
				} else if (serverResponce.lastIndexOf("Failed to Insert.Multiple rows does not allow Default Flag") > -1
					|| serverResponce.lastIndexOf("Please Insert one Attachment with Default Flag") > -1) {//Size of each file should not exceed 5000KB.
					serverResponce = serverResponce.replace(/['"]+/g, '');
					var dialogSplitMessage = dialogSplitIconText(serverResponce, "Y");
					var modalObj = {
						title: 'Message',
						body: dialogSplitMessage,
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {
								if (dataView == "GRID-VIEW") {

									if ($("#attachGridViewFlag").val() != null && $("#attachGridViewFlag").val() == "Y") {
										//fetchTabsData(masterId, selectedrowindex);
										//                                        refreshGridData(tabId);
									} else {
										fetchAttachmentsTabGridData(tabId);
									}
								}
							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("dataDxpSplitterValue", modalObj);
				} else {
					if (dataView == "GRID-VIEW") {

						if ($("#attachGridViewFlag").val() != null && $("#attachGridViewFlag").val() == "Y") {
							//fetchTabsData(masterId, selectedrowindex);
							refreshGridData(tabId);
						} else {
							fetchAttachmentsTabGridData(tabId);
						}
					} else {
						fetchAttachmentsTabData(tabId);
					}

					//                    fetchAttachmentsTabData(tabId);
				}
				$("body").css({ "pointer-events": "auto" });
			},
			'onStart': function() {
				$('#wait').show();
				$("body").css({ "pointer-events": "none" });
				$("#wait").css("display", "block");
				// $('#message').hide();
			}
		});
		//        $("#browseTdId_" + param).show();

		$(id).on('uploadEnd', function(event) {
			var args = event.args;
			var fileName = args.file;
			var serverResponce = args.response;

			$(id).hide();

			if (serverResponce.lastIndexOf("File is Empty,Can't be Uploaded.") > -1) {//File is Empty,Cann't be Uploaded.

				var baskettype = $("#baskettypehid").val();
				var modalObj = {
					title: 'Message',
					body: serverResponce,
				};
				var buttonArray = [
					{
						text: 'Close',
						click: function() {

						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
			} else if (serverResponce.lastIndexOf("Size of each file should not exceed 5000KB.") > -1) {//Size of each file should not exceed 5000KB.

				var modalObj = {
					title: 'Message',
					body: serverResponce,
				};
				var buttonArray = [
					{
						text: 'Close',
						click: function() {

						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
			} else if (serverResponce.lastIndexOf("Please Uplaod Image or PDF File Only.") > -1) {//Size of each file should not exceed 5000KB.

				var modalObj = {
					title: 'Message',
					body: serverResponce,
				};
				var buttonArray = [
					{
						text: 'Close',
						click: function() {

						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
			} else {
				getEnclosureList(encvalue, param);
			}
		});
		if (dataView == "GRID-VIEW") {
			$(browseId).click();
		}
	} else {
	}
}
function showPdf(id, tabId) {
	$("#pdfMM").css('display', 'block');
	$("#pdfMM").html("");
	alert("hijkj");
	var baskettype = $("#baskettypehid").val();
	console.log("baskettype::" + baskettype);
	var isOpera = !!window.opera || navigator.userAgent.indexOf(' OPR/') >= 0;
	var isFirefox = typeof InstallTrigger !== 'undefined';   // Firefox 1.0+
	var isSafari = Object.prototype.toString.call(window.HTMLElement).indexOf('Constructor') > 0;
	var isChrome = !!window.chrome && !isOpera;              // Chrome 1+
	var isIE = /*@cc_on!@*/false || !!document.documentMode;   // At least IE6
	var content = "";
	var deleteIcon = "";
	$('#addAttachmentId').hide();
	if (tabId == 'MM_ATTACHMENTS_OLD') {
		content = $('#pdfHiddenIdOld_' + id).val();
		deleteIcon = "";
	} else {
		content = $('#pdfHiddenId_' + id).val();
		deleteIcon = "<img src='images/delete.gif' id='deleteAttachmentId' class='visionDeleteAttachment' title='Delete' onclick=\"updateAttachments('delete', 'pdf','" + tabId + "')\" >";
	}
	$('#hiddenRowId').val(id);
	var pdfContent = "";
	var browserType = "";
	//var deleteIcon="<img src='images/delete.gif' id='deleteAttachmentId' class='visionDeleteAttachment' title='Delete' onclick=\"updateAttachments('delete', 'pdf','" + tabId + "')\" >";
	var role = $('#rolehid').val();
	var specModelNo = $('#SPEC_MODEL_NO').val();
	console.log("enc list:show pdf::" + role);
	var encEditable = $("#encEditable").val();
	if (encEditable != null && encEditable == 'N') {
		pdfContent = " <iframe id='thedialog' class='visionFormTheDialog' src='materialPDF?id=" + id + "&tabId=" + tabId + "&specModelNo=" + specModelNo + "' onload='showDeleteButton()' ></iframe>";
		$("#somediv").dialog({
			modal: true,
			title: '',
			width: 1100,
			height: 500,
			fluid: true,
			close: function() {
				$("#thedialog").attr('src', '');
			},
			open: function() {
				$(this).closest(".ui-dialog").addClass("visionFormImageView");
				$(".visionHeaderMain").css("z-index", "999");
				$(".visionFooterMain").css("z-index", "999");
			},
			beforeClose: function(event, ui) {
				$("#pdfMM").html("");
				//                $("#thedialog").attr('src', '');
				$(".visionHeaderMain").css("z-index", "99999");
				$(".visionFooterMain").css("z-index", "99999");
			}
		});
		$("#deleteAttachmentId").hide();
	} else {
		pdfContent = " <iframe id='thedialog' class='visionFormTheDialog' src='materialPDF?id=" + id + "&tabId=" + tabId + "&specModelNo=" + specModelNo + "'  ></iframe>";
		$("#somediv").dialog({
			modal: true,
			title: '',
			width: 1100,
			height: 500,
			fluid: true,
			close: function() {
			},
			open: function() {
				$(this).closest(".ui-dialog").addClass("visionFormImageView");
				$(".visionHeaderMain").css("z-index", "999");
				$(".visionFooterMain").css("z-index", "999");
			},
			beforeClose: function(event, ui) {
				$("#pdfMM").html("");
				//                $("#thedialog").attr('src', '');
				$(".visionHeaderMain").css("z-index", "99999");
				$(".visionFooterMain").css("z-index", "99999");
			}
		});
		$("#deleteIcon").html(deleteIcon);

		//  frameContent = "<iframe frameborder='0' height='100' width='100' src='tempFiles/" + content + "' style='border:solid 1px #000;' id='iframeid' onload='showDeleteButton()'/>";
	}
	console.log(pdfContent);

	$("#thedialog").show();

	$("#pdfMM").html(pdfContent);

}
function showImage(row_id, tabId) {
	alert(tabId);
	$('#deleteAttachmentId').hide();
	//  $('#deleteAttachmentId1').hide();
	$('#addAttachmentId').hide();
	$('#thedialog').hide();
	var baskettype = $("#baskettypehid1").val();
	var insertContent = "";
	var imgContent = "";
	if (tabId == "MM_ATTACHMENTS_OLD") {
		insertContent = $('#imageOld_' + row_id).attr("src");
		imgContent = "";
	} else {
		insertContent = $('#image_' + row_id).attr("src");
		imgContent = "<img src='images/delete.gif' id='deleteimgAttachmentId'  class='visionDeleteAttachment' title='Delete' onclick=\"updateAttachments('delete', 'image','" + tabId + "')\">";
	}

	var maincontent = "";
	// var imgContent="<img src='images/delete.gif' id='deleteimgAttachmentId'  class='visionDeleteAttachment' title='Delete' onclick=\"updateAttachments('delete', 'image','" + tabId + "')\">";
	console.log("baskettype::" + baskettype);
	var role = $('#rolehid').val();
	console.log("enc list:show Image::" + role);
	var encEditable = $("#encEditable").val();
	if (encEditable != null && encEditable == 'N') {
		console.log("IF SHOW IMAGE:::");
		$("#imgdialog").attr('src', insertContent);
		$("#deleteimgAttachmentId").hide();
		$("#imgdiv").dialog({
			modal: true,
			title: '',
			width: 1100,
			height: 500,
			fluid: true,
			close: function() {
			},
			open: function() {
				$(this).closest(".ui-dialog").addClass("visionFormImageView");
				$(".visionHeaderMain").css("z-index", "999");
				$(".visionFooterMain").css("z-index", "999");
			},
			beforeClose: function(event, ui) {
				$("#imgdialog").attr('src', '');
				$(".visionHeaderMain").css("z-index", "99999");
				$(".visionFooterMain").css("z-index", "99999");
			}
		});

	} else {
		$("#imgdialog").attr('src', insertContent);
		$("#imgdiv").dialog({
			modal: true,
			title: '',
			width: 1100,
			height: 500,
			fluid: true,
			close: function() {
			},
			open: function() {
				$(this).closest(".ui-dialog").addClass("visionFormImageView");
				$(".visionHeaderMain").css("z-index", "999");
				$(".visionFooterMain").css("z-index", "999");
			},
			beforeClose: function(event, ui) {
				$("#imgdialog").attr('src', '');
				$(".visionHeaderMain").css("z-index", "99999");
				$(".visionFooterMain").css("z-index", "99999");
			}
		});
		$("#deleteImg").html(imgContent);
	}
	$('#hiddenRowId').val(row_id);
	$("#imagedispid").html(maincontent);
}
//otp verification code//
function save_email_toDB(basicData) {//21222
	var verified_email = $("#email_id").val();
	ajaxStart();
	$.ajax({
		dataType: 'html',
		type: "POST",
		url: 'register',
		traditional: true,
		cache: false,
		data: {
			'verified_email': verified_email
		},
		success: function() {
			ajaxStop();
		}
	});
}
function sendOtp() {
	$(".otp_status").text("");
	var email = $("#email_id").val();
	$(".email_status").text(""); //16322 otp to email
	$("#again_otp").hide(); //17322
	$("#otp_textfield").css({ "border": "none", "border-bottom": "1px solid #ddd" }); //17322

	ajaxStart();
	$.ajax({
		datatype: 'html',
		type: "POST",
		url: 'emailOtpVerification',
		traditional: true,
		cache: false,
		data: {
			'email': email
		},
		success: function(result) {

			var resultObj = {};
			resultObj = JSON.parse(result);
			var message = resultObj['message'];
			var flag = resultObj['flag'];
			var mail_already_exists = resultObj['mail_already_exists'];
			if (mail_already_exists) {
				$("#form-total").find("button").show();
				$("#email_id").css({ "border": "#00FF00 solid 1px" }); //15322
				//11322
				$("#form-total").find("button").removeAttr("disabled");
			}
			if (flag) {
				ajaxStop();
				clearInterval(testInterval);
				cooldown();
				$("#otp_textfield").show();

				$("#verifyOtpBtn").show();
				$(".email_status").show();
				$(".email_status").removeClass("otp_status_red");
				$(".email_status").addClass("otp_status_green");
				$(".email_status").text("Otp sent successfully to: " + email);
			} else {
				ajaxStop();
				$(".email_status").show();
				$(".email_status").text(message);
				$(".email_status").addClass("otp_status_red");
			}
		}
	});
}

//17222
function verifyAddress(event) {
	var email = $("#email_id");
	$(".email_status").removeClass("otp_status_red"); //16322 otp to email_status
	$(".email_status").removeClass("otp_status_green");
	$("#form-total").find("button").attr("disabled", "true");
	$("#email_id").css({ "border": "none", "border-bottom": "1px solid #ddd" });
	$("#verifyOtpBtn").hide();
	$("#verifyOtpBtn").css({ "display": "none" });
	//    verifyOtpBtn
	//                        $("#email_id").css({"border": "none", "border - bottom": "1px solid #ddd"});
	if ((/^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/.test(email.val()))) {
		$("#first_verify_acc").show();
		//        $("#verifyOtpBtn").show();15322 changed the id from button to a in jsp
		$("#form-total").find("button").hide();
		$("#activate_account").show(); //7322
	} else {
		$("#first_verify_acc").hide();
		$("#verifyOtpBtn").hide();
		//        $("#form-total").find(".btn").show();
		$(".email_status").hide();
		$("#activate_account").hide(); //7322
	}
}
//18222
function verify_email_otp() {
	$("#verifyOtpBtn").hide();
	$(".otp_status").text("");
	$(".otp_status").removeClass("otp_status_green");
	$(".otp_status").removeClass("otp_status_red");

	var otpValue = $("#otp_textfield").val();
	//    var recievedotp = $("#otp_gen").val();
	$.ajax({
		dataType: 'html',
		type: "POST",
		url: 'checkOtpVal',
		traditional: true,
		cache: false,
		data: {
			'otpValue': otpValue
		},
		success: function(valid) {
			// if (otpValue == recievedotp) {
			var validVar = JSON.parse(valid);
			$("#otp_textfield").val(""); //17322
			if ($("#timer").val() == 0) {//21322 for expiry otp
				validVar = 0;
				//                $(".otp_status").removeClass("otp_status_green");
				$(".otp_status").addClass("otp_status_red");
				$(".otp_status").text("OTP expired please send the OTP again");
				$("#first_verify_acc").show();
				$("#verifyOtpBtn").show(); //16322
				$("#timer").hide(); //17322
				$("#again_otp").show(); //17322
				$("#otp_textfield").css({ "border": "#ff2C2C solid 1px" }); //16322

			} else if (validVar.check) {
				clearInterval(testInterval);
				cooldown(1);
				$(".email_status").hide(); //16322
				//                $(".otp_status").removeClass("otp_status_red");
				$(".otp_status").addClass("otp_status_green");
				$(".otp_status").text("Account Verified");
				$("#form-total").find("button").show();
				$("#verifyOtpBtn").hide();
				$("#otp_textfield").hide();
				$(".otp_notify").show();
				$("#first_verify_acc").hide();
				//                #00FF00 #1faf1f
				$("#email_id").css({ "border": "#00FF00 solid 1px" });
				$("#form-total").find("button").removeAttr("disabled");
			} else {
				$(".otp_status").text("");
				//                $(".otp_status").removeClass("otp_status_green");
				$(".otp_status").addClass("otp_status_red");
				$(".otp_status").text("OTP was Incorrect");
				$("#first_verify_acc").show();
				$("#verifyOtpBtn").show(); //16322
				$("#timer").hide(); //17322
				$("#again_otp").show(); //17322

				//                $("#first_verify_acc").text("Verify Again");
				$("#otp_textfield").css({ "border": "#ff2C2C solid 1px" }); //16322
			}
		}
	});
}

function cooldown(breakCooldown) {
	//    aparently u cant write int in forloop in js use let instead
	$("#first_verify_acc").hide();
	$("#timer").show();
	var stopTimer = 0;
	if (breakCooldown == 1) {
		$("#again_otp").hide();
		$("#timer").hide();
		$("#again_otp").hide();
		stopTimer = 1;
	} else {
		var c = 120;
		testInterval = setInterval(function() {
			if (c >= 0) {
				$("#timer").val(c);
				c--;
			}
			if (c == 0 && stopTimer == 0) {
				$("#timer").hide();
				$("#again_otp").show();
			}
		}
			, 1000);
	}
}
//otp verification code//
$(document).ready(function() {
	var redirectFromActiveUser = window.location.search;
	if (redirectFromActiveUser != '' && redirectFromActiveUser.includes("id")) {
		$('#accountActivationModal').modal('show');
		//        setTimeout(function () {
		//            window.close();
		//        }, 10000);
	}
});
function activateAccount() {//7322
	var email = $("#email_id").val();
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: 'accountActivation',
		traditional: true,
		cache: false,
		data: {
			email: email,
		},
		success: function(data, textStatus, jqXHR) {
		}
	});
}
function sideMenuAccess(ssRole) {
	var ssUserName = $("#ssUsername").val();
	if (ssUserName == null || ssUserName == undefined || ssUserName == '') {
		console.log("hypothesys BreakDown");
		var modalObj = {
			title: 'Message',
			body: "Please signin to access the services"
		};
		var buttonArray = [
			{
				text: 'Sign In',
				click: function() {
					showUserLoginDetails("isMainSigninBtn");
				},
				isCloseButton: true
			}

		];
		modalObj['buttons'] = buttonArray;
		createModal("dataDxpSplitterValue", modalObj);
		setTimeout(stopLoader, 1000);
	}
}
function activationMailToUser(userName) {
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: "activateUserThroughLink",
		traditional: true,
		cache: false,
		data: {
			userName: userName,
		},
		success: function(response) {
			ajaxStop();
			if (response != null && !jQuery.isEmptyObject(response)) {
				var modalObj = {
					title: 'Message',
					body: response['message']
				};
				var buttonArray = [
					{
						text: 'Close',
						click: function() {
							if (response['messageFlag']) {
								window.location.href = "https://insights.intellisensesolutions.com/";
							} else {
								$("#myModal").css("display", "none");
							}
						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("myModal", modalObj);
			}
		},
	});
}
function showUserLoginDetails(id) {
	$("#" + id).hide();

	$("#isUsernameShowHide").show();
	$("#isNextBtnShowHide").show();
	$("#isBackShowHide").show();
	$("#rsUsername").focus();
	$(".signUpBtnDiv").hide();
}
function goBackChanges() {
	var backFlag = $("#afterUserFlag").val();
	if (backFlag != null && backFlag != "" && backFlag != undefined && backFlag == 'N') {
		$("#isMainSigninBtn").show();
		$("#isMainSignupBtn").show();
		$("#isNextBtnShowHide").hide();
		$("#isUsernameShowHide").hide();
		$("#isBackShowHide").hide();
		$("#loginError").hide();
	} else if (backFlag != null && backFlag != "" && backFlag != undefined && backFlag == 'Y') {
		$("#isFrgtPwedBtnShowHide").hide();
		$("#isPasswordShowHide").hide();
		$("#isMainSignupBtn").hide();
		$("#isMainSigninBtn").hide();
		$("#isNextBtnShowHide").show();
		$("#isSigninBtnShowHide").hide();
		$("#isUsernameShowHide").show();
	} else {

	}
}
function showOrHideMsg(id) {
	$("#" + id).hide();
	if (event.which === 13) {
		loginOpeartions()
	}
}
function showNextPassword() {
	var userName = $("#rsUsername").val();
	if (userName != null && userName != undefined && userName != '') {
		processLoginAuth(userName);
	} else {
		$("#loginError").html("Username cannot be empty");
	}

}
var convCalled = false;
function chatApplication() {
	console.log("clicked");
	if (!convCalled) {
		chatWindow.talk(convo);
	}
	convCalled = true;

	$('.chatBox').toggle({
		height: 'toggle'
	}, 400);

}

function minimizeChatBot() {
	console.log(1);
	var chatBotHeight = $(".bubble-container").height();
	if (chatBotHeight >= 548 && chatBotHeight <= 550) {
		$(".chatBoxLargeSpaced").addClass("chatbotShrinked").removeClass("chatBoxLargeSpaced");
		console.log(chatBotHeight);
		$(".bubble-container").animate({
			height: 60
		}, 400);
		// $(".chatBotHeaderBottom").css("display", "none");
		$(".chatBotHeaderTop").css("height", 60);
		$(".input-wrap").css("display", "none");
		$("#maxminIcon").attr("src", "images/maximize.png");
		$("#maxminIcon").attr("title", "maximize");
	} else {
		$(".chatbotShrinked").removeClass("chatbotShrinked").addClass("chatBoxLargeSpaced");
		console.log("not eqal to 400");
		$(".bubble-container").animate({
			height: 400
		}, 400);
		// $(".chatBotHeaderTop").css("height", 30);
		// $(".bubble-wrap").css("top", 110);
		$(".input-wrap").css("display", "block");
		$("#maxminIcon").attr('src', 'images/minimize.png');
		$("#maxminIcon").attr("title", "minimize");
	}
}



function closeChatBot() {
	console.log("closed");
	localStorage.removeItem("chatBotLanguage");
	$(".bubble").remove();
	$('.chatBox').hide();
	convCalled = false;
}




var convo = {
	ice: {
		says: ["Hello there! Welcome to SmartBI", "Are you willing to know about our solution?",],
		reply: [
			{
				question: "Yes",
				answer: "YES1"
			},
			{
				question: "No",
				answer: "NO1"
			}
		],
	}

}
var convCalled = false;
var i = 0;
var chatWindow;
function chatApplication() {
	chatWindow = new Bubbles(document.getElementById('chat'), 'chatWindow', {
		inputCallbackFn: function(o) {
			sendMessage(o.input, chatWindow);
			i++;
		},
		responseCallbackFn: function(r) {
			sendMessage(r, chatWindow);
		}
	});

	console.log("clicked");
	if (!convCalled) {
		chatWindow.talk(convo);
	}
	convCalled = true;

	var flag = $('#chatBotIcon').attr('data-flag');
	/*if(flag == "A"){
		$(".userLoginChatbot").hide();
		$(".bubble-wrap").show();
		$(".input-wrap").show();
	} else{
	$(".bubble-wrap").hide();
	$(".input-wrap").hide();
	$(".userLoginChatbot").remove();
	$("#chat").append('<div class="userLoginChatbot" id="userLogin-Chatbot" style="margin-top:100px;margin-left: 20px;margin-right: 20px;">' 
	+ '<input type="email" name="chatBotEmail" id="chatBotEmail" value="" style="width:100%;padding:12px 20px;margin: 8px 0;display:inline-block;border: 1px solid #ccc;border-radius: 4px;box-sizing: border-box;" placeholder="email" required>'
	+'<p id="emailValMsg" style="color: red;"></p>'
	+ '<input type="text" name="chatBotUsername" id="chatBotUsername" value="" style="width:100%;padding:12px 20px;margin: 8px 0;display:inline-block;border: 1px solid #ccc;border-radius: 4px;box-sizing: border-box;" placeholder="username" required>'
	+'<p id="usernameValMsg" style="color: red;"></p>'
	+'<button type="button" class="btn btn-primary btn-lg btn-block" onclick="displayChatbot()">Start Chat</button>'
	+'<p id="chatValMsg" style="color: red;"></p>'
	+'<input type="hidden" id="loginSessionId" name="loginSessionId" val=""></div>')
	$("#chat").children(".userLoginChatbot").show();
	//$('.userLoginChatbot').attr('data-flag', 'A');

	}*/


	$('.chatBox').toggle({
		height: 'toggle'
	}, 400);

	$('.bubble-typing').hide();

}

function chatWindow_answer(reply) {
	/*var convocation1 = {
		ice:{
			says:["Hello,Im chat","plz select any one of the below"],
			reply:[
				{
					question:"Analytics",
					answer:"Analytics"
				},
				{
					question:"Integration",
					answer:"Integration"
				}
			],
			
		}
	};*/
	$(".input-wrap").find("textarea").attr("disabled", "true");
	$('.bubble-typing').show();
	var [lang, sessionId] = getChatbotParams();
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: "getChatBotResponse",
		traditional: true,
		cache: false,
		data: {
			message: reply,
			lang: lang,
			sessionId: sessionId
		},
		success: function(response) {
			ajaxStop();
			if (response != null && !jQuery.isEmptyObject(response)) {
				chatWindow.talk(response);
				var lang = response['ice'].lang;
				localStorage.setItem("chatBotLanguage", lang);
				var says = response['ice'].says;
				if (says != null && !jQuery.isEmptyObject(says)) {
					says = says[0];
					if (says != null && says != '' && says != undefined
						&& says == 'Thank you') {
						closeChatBot();
					}
				}
			}
			setTimeout(function() {

				$('.bubble-typing').hide();
			}, 2000);
			setTimeout(function() {
				$(".input-wrap").find("textarea").removeAttr('disabled');
			}, 4000);

		},
	});

}



function sendMessage(msg, chatWindow) {

	var [lang, sessionId] = getChatbotParams();
	$(".input-wrap").find("textarea").attr("disabled", "true");
	$('.bubble-typing').show();
	$(".input-wrap").find("textarea").val("");
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: "getChatBotResponse",
		traditional: true,
		cache: false,
		data: {
			message: msg,
			lang: lang,
			sessionId: sessionId
		},
		success: function(response) {
			ajaxStop();
			if (response != null && !jQuery.isEmptyObject(response)) {
				if (response['ice'].urls != null && !jQuery.isEmptyObject(response['ice'].urls)) {
					var url = response['ice'].urls[0];
					var list = "";
					if (url != undefined) {
						var urlen = url.length
						for (var i = 0; i < urlen; i++) {
							list += "<a href=" + url[i] + " style=\"color:blue;\" target=\"_blank\">" + url[i] + "</a></br>"
						}
						var say = response['ice'].says;
						if (say != null && !jQuery.isEmptyObject(say)) {
							say = say[0] + "</br>" + list;
							response['ice'].says[0] = say;
						}
					}
				}
				chatWindow.talk(response);
				var lang = response['ice'].lang;
				localStorage.setItem("chatBotLanguage", lang);
				var says = response['ice'].says;
				if (says != null && !jQuery.isEmptyObject(says)) {
					says = says[0];
					if (says != null && says != '' && says != undefined
						&& says == 'Thank you') {
						closeChatBot();
					}
				}
			}
			setTimeout(function() {
				$('.bubble-typing').hide();
			}, 2000);
			setTimeout(function() {
				$(".input-wrap").find("textarea").removeAttr('disabled');
			}, 4000);
		},
	});
}
function getChatbotParams() {
	//	var resultObj;
	var lang = localStorage['chatBotLanguage'];
	var sessionId;
	var cond = $(document).find('.signupClass').html();
	if (cond != null && cond != "" && cond != undefined) {

		if (cond.includes("jsessionid")) {
			var sessionAttr = $('.signupClass').children()[0].href;

			var sessionIdStr = sessionAttr.split(';')[1].split('.')[0];
			var jsessionId = sessionIdStr.split('=')[1]
			sessionId = jsessionId;
			localStorage.setItem("jsessionId", jsessionId);

		} else if (cond.includes("cloudRegistrationForm") && localStorage['jsessionId'] == undefined) {
			sessionId = localStorage['newLoginSession'];
		}
		else if (localStorage['jsessionId'] != null && localStorage['jsessionId'] != "" && localStorage['jsessionId'] != undefined) {
			sessionId = localStorage['jsessionId'];
		} else {
			var loginSession = localStorage['loginSession'];
			var newLoginSession = localStorage['newLoginSession'];
			var flag = $('#chatBotIcon').attr('data-flag');
			sessionId = loginSession;
			if (flag == "A") {
				sessionId = loginSession;
			} else {
				sessionId = newLoginSession;
			}
		}
	}
	else {
		var loginSession = localStorage['loginSession'];
		var newLoginSession = localStorage['newLoginSession'];
		var flag = $('#chatBotIcon').attr('data-flag');
		if (flag == "A") {
			sessionId = loginSession;
		} else {
			sessionId = newLoginSession;
		}
	}
	//	resultObj = {
	//		"sessionId": sessionId,
	//		"lang": lang
	//	}
	return [lang, sessionId];


}

function showorHidePassError(event) {

	if (event.which === 13) {
		loginOpeartions2()
	}

}



function processLoginAuth(rsUsername) {
	showLoader();
	var user = rsUsername;
	if (user != null && user != '' && user != undefined) {
		user = user.toUpperCase().trim();
	}else{
		return;
	}
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: 'getProcessLoginAuth',
		traditional: true,
		cache: false,
		data: {
			userName: user
		},
		success: function(response) {
			stopLoader();
			var message = response['message'];
			var messageFlag = response['messageFlag'];
			localStorage.setItem("userName", user);
			if (messageFlag === false) {
				$("#loginError").html(message);
				var passwordFlag = response['passwordFlag'];
				var status = response['status'];
				if (status == "INACTIVE") {
					$("#loginError").html(message);
					return false;
				}
				if (passwordFlag == "N" || passwordFlag == "R") {
					$("#processLoginID2").hide();
					$(".showorHidePassword").remove();
					$("#loginError").hide();
					resetPasswordForInActiveUsers(user, passwordFlag);
				}

			} else {

				var processVal = response['processVal'];
				if (processVal != null && processVal.includes("otp") && processVal.includes("captcha")) {
					localStorage.setItem("securityVerify", processVal)
					var OtpCode = response['OtpCode'];
					var captchaCode = response['captchaCode'];
					$("#verifyLoginSecurity").html(captchaCode);
					$("#verifyLoginSecurity").append(OtpCode);
					CreateCaptcha();
					$(".showorHidePassword").show();
					$("#showPassword").hide();
					$("#processLoginID2").show();
					$("#loginError").hide();
					$("#rsPassword").focus();
				} else if (processVal != null && processVal != "" && processVal != undefined && processVal.includes('captcha')) {
					var captchaCode = response['captchaCode'];
					localStorage.setItem("securityVerify", processVal);
					$("#verifyLoginSecurity").html(captchaCode);
					$("#showPassword").hide();
					$("#processLoginID2").show();
					$("#loginError").hide();
					$("#rsPassword").focus();
					CreateCaptcha();
					$(".showorHidePassword").show();
				} else if (processVal != null && processVal != "" && processVal != undefined && processVal == 'otp') {
					var OtpCode = response['OtpCode'];
					localStorage.setItem("securityVerify", processVal);
					$("#verifyLoginSecurity").html("");
					$("#verifyLoginSecurity").append(OtpCode);
					//                CreateCaptcha();
					$(".showorHidePassword").show();
					$("#showPassword").hide();
					$("#processLoginID2").show();
					$("#loginError").hide();
					$("#rsPassword").focus();

				} else if (processVal != null && processVal != "" && processVal != undefined && processVal == 'none') {
					localStorage.setItem("securityVerify", processVal);
					$("#verifyLoginSecurity").html("");
					$(".showorHidePassword").show();
					$(".LanguageSelector").show();
					$("#showPassword").hide();
					$("#processLoginID2").show();
					$("#loginError").hide();
					$("#rsPassword").focus();
				}

			}
		}
	});
}

function resetPasswordForInActiveUsers(userName, passwordFlag) {
	try {
		$("#userName").val(userName);
		var messagerstr = "";
		if (passwordFlag == 'N') {
			var messagerstr = "Please set your password";
		} else if (passwordFlag == 'R') {
			var messagerstr = "Please reset your password";
			//var messagerstr = "Your ID has been reset,Please set your password.";
		} else {
			var messagerstr = "Please set your password.";
		}

		var result = '<div class="changepassword" id="changepassword">'
			+ '<div class="titlepasswordClass row ">'
			+ '<div class="statusFlagErrorMessage ">' + messagerstr + '</div>'
			+ '<div class="col-md-4"><label data-error="wrong" data-success="right" for="form34">UserName:<sup style="color: red">*</sup></label></div>'
			+ '<div class="col-md-8 form-group">'
			+ "<span class=\"username\">" + userName + "</span>"
			+ '</div>'
			+ '<div class="col-md-4">'
			+ "<label data-error='wrong' data-success='right' for='form34'>New Password<sup style='color: red'>*</sup></label>"
			+ '</div>'
			+ '<div class="col-md-8 form-group eye_icon">'
			+ "<input autocomplete='off' type='password' name='password' id='newPassword' class='newPassword form-control' onblur=\"getgenericpasswordvalidation(id)\" placeholder='' title='EX:New Password' data-pswdregex=\"(.){8,15}:::[0-9]{1,}:::[a-z]{1,}:::[A-Z]{1,}:::([!,%,&amp;,@,#,$,^,*,?,_,~]{1,}):::(?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\\s).{8,15}:::\" data-pswddesc=\"Password must contain minimum of 8 characters:::Password must contain one number(0-9):::Password must contain one lowercase letter (a-z):::Password must contain one Uppercase letter (A-Z):::Password must contain one Special Character:::Enter Password As Specified:::\"> <i class='fa fa-eye-slash' id='neweyeshowhide' onclick='showNewPassword()' aria-hidden='true'></i>"
			+ "<span id=\"error_pwd\" class=\"all_errors\"></span>"
			+ "<div id='dis_newPassword' class='all_errors'></div>"
			+ '</div>'
			+ '<div class="col-md-4">'
			+ "<label data-error='wrong' data-success='right' for='form34'>Confirm Password<sup  style='color: red'>*</sup></label>"
			+ '</div>'
			+ '<div class="col-md-8 form-group eye_icon">'
			+ "<input autocomplete=\"off\" type=\"password\" name=\"password\" id=\"confirmPassword\" class='confirmPassword form-control' onblur=\"return checkPasswordMatchReType()\" placeholder=\"\" title=\"\" data-pswdregex=\"(.){7,15}:::[0-9]{1,}:::[a-z]{1,}:::[A-Z]{1,}:::([!,%,&amp;,@,#,$,^,*,?,_,~]{1,}):::(?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\\s).{8,15}:::\" data-pswddesc=\"Password must contain minimum of 8 characters:::Password must contain one number(0-9):::Password must contain one lowercase letter (a-z):::Password must contain one Uppercase letter (A-Z):::Password must contain one Special Character:::Enter Password As Specified:::\"><i class='fa fa-eye-slash' id='confirmeyeshowhide' onclick='showConfirmPassword()' aria-hidden='true'></i>"
			+ "<span id=\"error_password2\" class=\"all_errors\"></span>"
			+ "<div id='dis_confirmPassword' class='all_errors'></div>"
			+ '</div>'
			+ "<div class=\"visionChangePasswordData visionErrorCells col-12\" >"
			+ "<span id=\"error_password2\" class=\"all_errors\"></span>"
			+ "<div id=\"dis_password2\" class=\"all_errors\"></div></div>"
			+ "<div class=\"visionChangePasswordData visionErrorCells col-12\"> <input type=\"text\" style=\"display: none\" value=\"\" id=\"pers_Id\"></div>"
			+ "</div>"
			+ '</div>';
		var modalObj = {
			title: labelObject['Message'] != null ? labelObject['Message'] : 'Reset Password',
			body: result
		};
		var buttonArray = [
			{
				text: 'Update Password',
				click: function() {
					resetPassword(userName)
				},
				isCloseButton: false
			},
			{
				text: 'Cancel',
				click: function() {
				},
				isCloseButton: true
			}
		];
		modalObj['buttons'] = buttonArray;
		createModal("dataDxpSplitterValue", modalObj);
		$(".modal-backdrop").show();
		$(".modal-dialog").addClass("modal-md");
		$(".dataDxpSplitterValue").addClass("updatepasswordClass");
		//        passDescriptionShow();     
	} catch (e) {

	}


}



function CreateCaptcha() {
	var alpha = new Array('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9');
	var i;
	for (i = 0; i < 6; i++) {
		var a = alpha[Math.floor(Math.random() * alpha.length)];
		var b = alpha[Math.floor(Math.random() * alpha.length)];
		var c = alpha[Math.floor(Math.random() * alpha.length)];
		var d = alpha[Math.floor(Math.random() * alpha.length)];
		var e = alpha[Math.floor(Math.random() * alpha.length)];
		var f = alpha[Math.floor(Math.random() * alpha.length)];
	}
	cd = a + ' ' + b + ' ' + c + ' ' + d + ' ' + e + ' ' + f;
	$('#CaptchaImageCode').empty().append('<div id="CapCode" class="capcode" width="200" height="70">' + cd + '</div>')
}



function otpSend(username) {
	ajaxStart();
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: 'getProcessLoginOtpAuth',
		traditional: true,
		cache: false,
		data: {
			userName: username
		},
		success: function(response) {
			ajaxStop();
			var status = response['status'];
			$("#processLoginID2").show();
			if (status != null && status != "" && status != undefined && status == 'success') {
				var otpInput = response['otpInput'];
				var OtpCode = response['otp'];
				localStorage.setItem("otp", OtpCode);
				$("#verifyLoginSecurity").html(otpInput);
				$("#resenddiv").hide();
				$("#otpTextsSuccess").show();
				$("#timerText").show();
				reloadOtpBtn()
			} else {
				var otpInput = response['otpInput'];
				var OtpCode = response['otp'];
				localStorage.setItem("otp", OtpCode);
				$("#verifyLoginSecurity").html(otpInput);
				$("#otpErrorrText").html("Could not generate OTP please contact to Administration");
			}
		}
	});
}

function userEnterSecurity(status) {
	var securityStatus = status.toUpperCase();
	var passFild = $("#rsPassword").val();
	if (securityStatus != null && passFild != "" && securityStatus != undefined && securityStatus == 'CAPTCHA') {
		$("#passwordError").html("");
		var captcha = $("#CapCode").text();
		var val = $("#captchaUserCode").val();
		if (val == captcha.replaceAll(' ', '')) {
			$("#captchaErrorrText").html("<blink style='color:green;'>Correct " + securityStatus + "</blink>");
			loginOpeartions2();
		} else {
			$("#captchaErrorrText").html("<blink style='color:red;'>Incorrect " + securityStatus + "</blink>");
		}
	} else if (securityStatus != null && passFild != "" && securityStatus != undefined && securityStatus == 'OTP') {
		$("#passwordError").html("");
	} else {
		$("#passwordError").html("<blink>Password cannot be empty</blink>");
		$(".showorHidePassword").css('border', '2px solid red')
		$('#' + status + 'UserCode').prop("disabled", true);
		$("#otpTextsSuccess").hide();
		$("#otpErrorrText").html("");
	}
}

function showorHideError(status, passFlag) {
	var securityStatus = localStorage['securityVerify'];
	if (status == 'username') {
		if (event.which === 13) {
			showNextPassword();
		}
	} else if (status == 'password') {
		$('#' + securityStatus + 'UserCode').removeAttr("disabled");
		$("#passwordError").html("");
		if (securityStatus == 'none') {
			if (event.which === 13) {
				loginOpeartions2();
			} else if (passFlag != null && passFlag != "" && passFlag != undefined && passFlag == 'P') {
				loginOpeartions2();
			}

		} else if (securityStatus == 'captcha') {
			if (event.which === 13) {
				$("#captchaUserCode").focus();
				var passFild = $("#rsPassword").val();
				if (passFild != '') {
					var captcha = $("#CapCode").text();
					var val = $("#captchaUserCode").val();
					if (val == captcha.replaceAll(' ', '')) {
						loginOpeartions2()
					} else if (val == '') {
						$("#captchaErrorrText").html("<blink style='color:red;'>Captcha cannot be empty</blink>");
						$(".CaptchaTxtField").css('border', '2px solid red')
					} else if (val != '') {
						$("#captchaErrorrText").html("<blink style='color:red;'>Incorrect</blink>");
					}
				} else {
					$("#passwordError").html("<blink>Password cannot be empty</blink>");
				}
			} else {
				var captcha = $("#CapCode").text();
				var val = $("#captchaUserCode").val();
				if (val == captcha.replaceAll(' ', '')) {
					loginOpeartions2()
				}
				//                else if (val == '') {
				//                    $("#captchaErrorrText").html("<blink style='color:red;'>Captcha cannot be empty</blink>");
				//                } else if (val != '') {
				//                    $("#captchaErrorrText").html("<blink style='color:red;'>Incorrect</blink>");
				//                }
			}
		} else if (securityStatus == 'otp') {
			var otpField = $("#otpUserCode").val();
			if (otpField == undefined) {
				if (event.which === 13) {
					$("#otpButtonErrorrText").html("<blink style='color:red;'>Please press OTP Button</blink>");
				}
			} else {
				if (event.which === 13) {
					security = localStorage['otp'];
					if (security == btoa(otpField)) {
						loginOpeartions2()
					} else {
						$("#otpTextsSuccess").hide();
						$("#otpErrorrText").html("<blink style='color:red;'>otp cannot be empty</blink>");
					}
				} else {
					security = localStorage['otp'];
					if (security == btoa(otpField)) {
						loginOpeartions2()
					} else if (security != btoa(otpField) && otpField != '') {
						$("#otpTextsSuccess").hide();
						$("#otpErrorrText").html("<blink style='color:red;'>Incorrect OTP</blink>");
					} else {
						$("#otpTextsSuccess").hide();
						$("#otpErrorrText").html("<blink style='color:red;'>otp cannot be empty</blink>");
					}
				}
			}
		}
	}
}


function reloadOtpBtn() {
	var timeIntrvalConstituency = 60;
	var interval = setInterval(function() {
		timeIntrvalConstituency--;
		$("#timeSession").html(timeIntrvalConstituency);
		if (timeIntrvalConstituency === 0) {
			localStorage.setItem("otp", "");
			$("#resenddiv").show();
			$("#timeSession").html("");
			$("#otpTextsSuccess").hide();
			$("#timerText").hide();
			$('#otpUserCode').prop("disabled", true);
			clearInterval(interval);
		}
	}, 1000);
}

function displayChatbot() {
	var email = $("#chatBotEmail").val();
	var username = $("#chatBotUsername").val();
	var regex = /^([a-zA-Z0-9_.+-])+\@(([a-zA-Z0-9-])+\.)+([a-zA-Z0-9]{2,4})+$/;
	var isEmail = regex.test(email);
	$("#emailValMsg").empty();
	$("#chatValMsg").empty();
	$("#usernameValMsg").empty();
	if (isEmail == false) {
		$("#emailValMsg").html("Please enter valid email address");
	}
	if (username == null || username == undefined || username == '') {
		$("#usernameValMsg").html("Please enter valid username");
	}
	if (username != null && username != undefined && username != '' && email != null && email != undefined && email != '') {
		if (isEmail) {
			$(".userLoginChatbot").hide();
			$(".bubble-wrap").show();
			$(".input-wrap").show();
		}
	} else {
		$("#chatValMsg").html("Please enter all the details");
	}

}

function registerForm() {

	var modalObj = {
		title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
		body: "<div style='display: flex;'><span><img src='images/SignUp.png' class='themeModeDark' style='width:22px;'></span><span style='font-weight:700;margin-left: 8px;'><h6>"
			+ "If your organization has a subscription, request your Admin or Manager to create a username for you. If there is no subscription, consider subscribing</h6></span></div>",
	};
	var buttonArray = [
		{
			text: 'Subscription',
			click: function() {
				window.open("https://smartintegraphics.com/", "_blank");
			},
			isCloseButton: true
		},
		{
			text: 'Cancel',
			click: function() {

			},
			isCloseButton: true
		}
	];
	modalObj['buttons'] = buttonArray;
	createModal("dataDxpSplitterValue", modalObj);
	$(".modal-dialog").addClass("modal-xs");
	//    $("#registerForm").attr("target", "_blank");
	//    $("#registerForm").attr("action", "cloudRegistrationForm");
	//    $("#registerForm").submit();
}



function showPassword() {
	var passAttribute = $("#oldpassword").attr("type");
	var eyeClass = $("#eyeshowhide").attr("class");
	if (passAttribute == "password") {
		$("#oldpassword").attr("type", "text");
		$("#eyeshowhide").attr("class", "fa fa-eye");
	} else {
		$("#oldpassword").attr("type", "password");
		$("#eyeshowhide").attr("class", "fa fa-eye-slash");
	}
}
function showNewPassword() {
	var passAttribute = $("#newPassword").attr("type");
	var eyeClass = $("#neweyeshowhide").attr("class");
	if (passAttribute == "password") {
		$("#newPassword").attr("type", "text");
		$("#neweyeshowhide").attr("class", "fa fa-eye");
	} else {
		$("#newPassword").attr("type", "password");
		$("#neweyeshowhide").attr("class", "fa fa-eye-slash");
	}
}
function showConfirmPassword() {
	var passAttribute = $("#confirmPassword").attr("type");
	var eyeClass = $("#confirmeyeshowhide").attr("class");
	if (passAttribute == "password") {
		$("#confirmPassword").attr("type", "text");
		$("#confirmeyeshowhide").attr("class", "fa fa-eye");
	} else {
		$("#confirmPassword").attr("type", "password");
		$("#confirmeyeshowhide").attr("class", "fa fa-eye-slash");
	}
}

function getgenericpasswordvalidation(ele) {

	var ele = ele;
	var str = $("#" + ele).val();
	var errorID = "#dis_" + ele;
	var regex;
	var desc;
	var patt;
	var dataRegex = $("#" + ele).attr("data-pswdRegex");
	var dataDesc = $("#" + ele).attr("data-pswdDesc");
	var user_name = $("#userName").val();
	if (str != null && str != '') {
		if (user_name != null && user_name != '' && user_name.toUpperCase() != str.toUpperCase()) {
			var pswdRegex = dataRegex.split(":::");
			var pswdDesc = dataDesc.split(":::");
			for (var i = 0; i < pswdRegex.length; i++) {
				regex = pswdRegex[i];
				patt = new RegExp(regex);

				if (!patt.test(str)) {
					var msg = pswdDesc[i];
					err_msg(errorID, msg);
					return false;
				} else {
					$(errorID).hide();
					$("#dis_newPassword").html("");
					$("#confirmPassword").prop('disabled', false);
					//            $("#restpassword").prop('disabled', true);
				}
			}
			return true;
		}
	} else {
		msg = "Password should not blank.";
		msg = labelObject[msg] != null ? labelObject[msg] : msg;
		err_msg(errorID, msg);
		return false;
	}

}

function checkPasswordMatchReType() {

	var pass = $("#newPassword").val();
	var confirmPassword = $("#confirmPassword").val();
	if (pass == confirmPassword) {
		$("#dis_confirmPassword").html("");


	} else {
		$("#dis_confirmPassword").show();
		$("#dis_confirmPassword").html(labelObject["Passwords do not match."] != null ? labelObject["Passwords do not match."] : "Passwords do not match.");
		//        $("#restpassword").prop('disabled', true);
	}

}
function resetPassword(userName) {
	try {
		var newPassword = $("#newPassword").val();
		var confirmPassword = $("#confirmPassword").val();
		if (!newPassword) {
			var id = "#dis_newPassword";
			var msg = "Enter New Password";
			err_msg(id, msg);
			return false;
		} else if (!confirmPassword) {
			var id = "#dis_confirmPassword";
			var msg = "Re-Type New Password";
			$("#new_password").text("")
			err_msg(id, msg);
			return false;
		} else if (newPassword != confirmPassword) {
			var id = "#dis_password2";
			var msg = "newPassword & confirmPassword should not match.";
			err_msg(id, msg);
			return false;
		} else if (newPassword == confirmPassword) {
			updateNewPasswordForInActive(userName);
		}
	} catch (e) {

	}


}

function updateNewPasswordForInActive(userName) {
	try {
		var secretKey = $('meta[name=keygeneration]').attr("content");
		var newPassword = $("#newPassword").val();
		var ssUsername = userName;
		var Confirm_newPas = $("#confirmPassword").val();
		var password = $("#newPassword").val();
		var encryptedPassword = CryptoJS.AES.encrypt(password, secretKey);
		$("#newPassword").val(encryptedPassword);

		var confirm_password = $("#confirmPassword").val();
		var encryptedConfirmPassword = CryptoJS.AES.encrypt(confirm_password, secretKey);
		$("#confirmPassword").val(encryptedConfirmPassword);
		showLoader();
		$.ajax({
			type: "POST",
			traditional: true,
			dataType: 'html',
			url: 'changepassword',
			traditional: true,
			cache: false,
			data: {
				password: $("#newPassword").val(),
				confirm_password: Confirm_newPas,
				ssUsername: ssUsername,
			},
			success: function(response) {
				stopLoader();
				if (response != null) {
					var MessageStatus = JSON.parse(response).message;
					var MessageFlag = JSON.parse(response).messageFlag;
				}
				var modalObj = {
					title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
					body: MessageStatus
				};
				var buttonArray = [
					{
						text: 'Ok',
						click: function() {
							if (MessageFlag == true) {
								logout()
							} else {
								updatePassword()
							}

						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
				$(".modal-dialog").addClass("modal-xs");


			}
		});
	} catch (e) {

	}


}




function updatePassForm() {

	var result = '<div class="changepassword" id="changepassword">'
		+ '<div class="titlepasswordClass row ">'
		+ '<div class="col-12" id="loginOtpMsg" style="display:none"></div>'
		+ '<div class="col-12"id="otpStatusMsg"></div>'
		+ '<div id=\"otp-wrapper\"></div>'

		+ '<div class="col-md-4 userHideLabel"><label  data-error="wrong" data-success="right" for="form34">User Name<sup style="color: red">*</sup></label></div>'
		+ '<div class="col-md-8 userHideLabel form-group"><div class=\"user-wrapper\"><input autocomplete=\"off\" type=\"text\" name=\"userNameText\" id=\"userNameText\" class="userNameText form-control " title=\"\"\"><button class="userValidClass btn btn-primary" onclick="forgotUserValidate()">Validate</button></div>'
		+ "<div id='userNameError'></div>"
		+ '</div>'
		+ '<div class="col-md-4"><label data-error="wrong" data-success="right" for="form34">Old Password<sup style="color: red">*</sup></label></div>'
		+ '<div class="col-md-8 form-group eye_icon"><input autocomplete=\"off\" type=\"password\" name=\"password\" id=\"oldpassword\" class="oldPassword form-control" onblur=\"genericpasswordvalidation(id)\" placeholder=\"\" title=\"\" data-pswdregex=\"(.){8,15}:::[0-9]{1,}:::[a-z]{1,}:::[A-Z]{1,}:::([!,%,&amp;,@,#,$,^,*,?,_,~]{1,}):::(?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\\s).{8,15}:::\" data-pswddesc=\"Password must contain minimum of 8 characters:::Password must contain one number(0-9):::Password must contain one lowercase letter (a-z):::Password must contain one Uppercase letter (A-Z):::Password must contain one Special Character:::Enter Password As Specified:::\">'
		//+' <i class="fa fa-eye-slash" id="eyeshowhide" onclick="showPassword()" aria-hidden="true"></i>'
		+ "<span id=\"error_old_password\" class=\"all_errors\"></span>"
		+ "<div id='dis_oldpassword' class='all_errors'></div>"
		+ '</div>'
		+ '<div class="col-md-4">'
		+ "<label data-error='wrong' data-success='right' for='form34'>New Password<sup style='color: red'>*</sup></label>"
		+ '</div>'
		+ '<div class="col-md-8 form-group eye_icon">'
		+ "<input autocomplete='off' type='password' name='password' id='newPassword' class='newPassword form-control' onblur=\"genericpasswordvalidation(id)\" placeholder='' title='EX:New Password' data-pswdregex=\"(.){8,15}:::[0-9]{1,}:::[a-z]{1,}:::[A-Z]{1,}:::([!,%,&amp;,@,#,$,^,*,?,_,~]{1,}):::(?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\\s).{8,15}:::\" data-pswddesc=\"Password must contain minimum of 8 characters:::Password must contain one number(0-9):::Password must contain one lowercase letter (a-z):::Password must contain one Uppercase letter (A-Z):::Password must contain one Special Character:::Enter Password As Specified:::\">"
		//+ "<i class='fa fa-eye-slash' id='neweyeshowhide' onclick='showNewPassword()' aria-hidden='true'></i>"
		+ "<span id=\"error_pwd\" class=\"all_errors\"></span>"
		+ "<div id='dis_newPassword' class='all_errors'></div>"
		+ '<div class="passwordDecription" style="display:none">'
		+ '<div class="gridd"><span id="8char" class="fa fa-times"></span><span class="text-body">min length 8</span><span id="caps" class="fa fa-times"></span>'
		+ '<span class="text-body">Uppercase</span><span id="spchar" class="fa fa-times"></span><span class="text-body">Special Character</span><span id="int" class="fa fa-times"></span><span class="text-body">number</span>'
		+ '</div></div>'
		+ '</div>'

		+ '<div class="col-md-4">'
		+ "<label data-error='wrong' data-success='right' for='form34'>Confirm Password<sup  style='color: red'>*</sup></label>"
		+ '</div>'
		+ '<div class="col-md-8 form-group eye_icon">'
		+ "<input autocomplete=\"off\" type=\"password\" name=\"password\" id=\"confirmPassword\" class='confirmPassword form-control' onblur=\"return checkPasswordMatchReType()\" placeholder=\"\" title=\"\" data-pswdregex=\"(.){7,15}:::[0-9]{1,}:::[a-z]{1,}:::[A-Z]{1,}:::([!,%,&amp;,@,#,$,^,*,?,_,~]{1,}):::(?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\\s).{8,15}:::\" data-pswddesc=\"Password must contain minimum of 8 characters:::Password must contain one number(0-9):::Password must contain one lowercase letter (a-z):::Password must contain one Uppercase letter (A-Z):::Password must contain one Special Character:::Enter Password As Specified:::\">"
		//+"<i class='fa fa-eye-slash' id='confirmeyeshowhide' onclick='showConfirmPassword()' aria-hidden='true'></i>"
		+ "<span id=\"error_password2\" class=\"all_errors\"></span>"
		+ "<div id='dis_confirmPassword' class='all_errors'></div>"
		+ '</div>'
		+ "<div class=\"visionChangePasswordData visionErrorCells col-12\" >"
		+ "<span id=\"error_password2\" class=\"all_errors\"></span>"
		+ "<div id=\"dis_password2\" class=\"all_errors\"></div></div>"
		+ "<div class=\"visionChangePasswordData visionErrorCells col-12\"> <input type=\"text\" style=\"display: none\" value=\"\" id=\"pers_Id\"></div>"
		+ "</div>"
		+ '</div>';
	var modalObj = {
		title: labelObject['Message'] != null ? labelObject['Message'] : 'Change Password',
		body: result
	};
	var buttonArray = [
		{
			text: 'Apply Changes',
			click: function() {
				checkpasswordchange('Y');

			},
			isCloseButton: false
		},
		{
			text: 'Cancel',
			click: function() {
			},
			isCloseButton: true
		}
	];
	modalObj['buttons'] = buttonArray;
	createModal("dataDxpSplitterValue", modalObj);
	$(".modal-backdrop").show();
	$(".modal-dialog").addClass("modal-md");
	$(".dataDxpSplitterValue").addClass("updatepasswordClass");
	//    $(".modal-header").append('<img class="logo" src="images/PilogCloudRedBlue.gif">');
	passDescriptionShow();
	removeCopyPasteEvent();
	$("#oldpassword").prop("readonly", true);
	$("#newPassword").prop("readonly", true);
	$("#confirmPassword").prop("readonly", true);

}
function checkpasswordchange(loginFlag) {
	if (loginFlag == 'N') {
		var newPassword = $("#newPassword").val();
		var Confirm_newPas = $("#confirmPassword").val();
		if (!newPassword) {
			var id = "#dis_newPassword";
			var msg = "Enter New Password";
			//        $("#old_password").text("")
			errorMessage(id, msg);
			return false;
		} else if (!Confirm_newPas) {
			var id = "#dis_confirmPassword";
			var msg = "Re-Type New Password";
			$("#new_password").text("")
			errorMessage(id, msg);
			return false;
		} else if (Confirm_newPas != newPassword) {
			var id = "#dis_password2";
			var msg = "Confirm password and New password are not same";
			errorMessage(id, msg);
			return false;
		} else if (!genericpasswordvalidation("newPassword", loginFlag)) {
			return false;
		} else if (newPassword == Confirm_newPas) {
			updateNewPassword(loginFlag);
		}
	} else {
		var oldPassword = $("#oldpassword").val();
		var newPassword = $("#newPassword").val();
		var Confirm_newPas = $("#confirmPassword").val();
		if (!oldPassword) {
			var id = "#dis_oldpassword";
			var msg = "Enter Old Password";
			errorMessage(id, msg);
			return false;
		} else if (!newPassword) {
			var id = "#dis_newPassword";
			var msg = "Enter New Password";
			$("#old_password").text("")
			errorMessage(id, msg);
			return false;
		} else if (!Confirm_newPas) {
			var id = "#dis_confirmPassword";
			var msg = "Re-Type New Password";
			$("#new_password").text("")
			errorMessage(id, msg);
			return false;
		} else if (oldPassword == newPassword) {
			var id = "#dis_password2";
			var msg = "Old password and new password are same";
			errorMessage(id, msg);
			return false;
		} else if (Confirm_newPas != newPassword) {
			var id = "#dis_password2";
			var msg = "Confirm password and New password are not same";
			errorMessage(id, msg);
			return false;
		} else if (!genericpasswordvalidation("newPassword")) {
			return false;
		} else if (newPassword == Confirm_newPas) {
			updateNewPassword(loginFlag);
		}
	}

}

function updateNewPassword(loginFlag) {
	var secretKey = $('meta[name=keygeneration]').attr("content");
	var newPassword = $("#newPassword").val();
	var ssUsername = $("#ssUsername").val();
	if (ssUsername == null || ssUsername == '' || ssUsername == undefined) {
		ssUsername = $("#userNameText").val();
		$("#ssUsername").val(ssUsername);
	}
	var Confirm_newPas = $("#confirmPassword").val();

	try {
		var old_password = $("#oldpassword").val();
		var encryptedOldPassword = CryptoJS.AES.encrypt(old_password, secretKey);
		$("#oldpassword").val(encryptedOldPassword);
	} catch (ep) {

	}


	var password = $("#newPassword").val();
	var encryptedPassword = CryptoJS.AES.encrypt(password, secretKey);
	$("#newPassword").val(encryptedPassword);

	var confirm_password = $("#confirmPassword").val();
	var encryptedConfirmPassword = CryptoJS.AES.encrypt(confirm_password, secretKey);
	$("#confirmPassword").val(encryptedConfirmPassword);
	showLoader();
	$.ajax({
		type: "POST",
		traditional: true,
		dataType: 'html',
		url: 'changepassword',
		traditional: true,
		cache: false,
		data: {
			old_password: $("#oldpassword").val(),
			password: $("#newPassword").val(),
			confirm_password: Confirm_newPas,
			ssUsername: ssUsername.toUpperCase(),
			loginFlag: loginFlag,
		},
		success: function(response) {

			stopLoader();
			if (response != null) {
				var oldPassValid = JSON.parse(response).oldPassValid;
				var MessageStatus = JSON.parse(response).message;
				var MessageFlag = JSON.parse(response).messageFlag;
			}
			if (oldPassValid == false) {
				$("#oldpassword").val("");
				$("#newPassword").val("");
				$("#confirmPassword").val("");

				var errorID = "#dis_oldpassword";
				msg = labelObject[MessageStatus] != null ? labelObject[MessageStatus] : MessageStatus;
				err_msg(errorID, msg);
				$('#dis_oldpassword').show();
				return false;
			} else {
				var modalObj = {
					title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
					body: MessageStatus
				};

				var buttonArray = [
					{
						text: 'Ok',
						click: function() {
							if (MessageFlag == true) {
								logout()
							} else {
								updatePassword()
							}

						},
						isCloseButton: true
					}
				];

				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
				$(".modal-dialog").addClass("modal-xs");
			}


		}
	});
}
function passDescriptionShow() {
	$("#newPassword").focus(function() {
		$(".passwordDecription").show(500);
	});
	$("#newPassword").focusout(function() {
		$(".passwordDecription").hide(500);
	});
	var password = document.getElementById("newPassword");
	password.onkeyup = function checkval(password) {
		password = document.getElementById("newPassword");
		var char8 = document.getElementById("8char");
		var uper = document.getElementById("caps");
		var spchar = document.getElementById("spchar");
		var int = document.getElementById("int");

		var ucase = new RegExp("[A-Z]+");

		const specialChars = /[`!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?~]/;
		var num = new RegExp("[0-9]+");
		if (password.value.length >= 8) {
			char8.classList.remove("fas", "fa-times");
			char8.classList.add("fas", "fa-check-circle");
		} else if (password.value.length < 8) {
			char8.classList.remove("fas", "fa-check-circle");
			char8.classList.add("fas", "fa-times");

			showErrorText(password, `${getFieldNameChange(password)} must be at least 8 characters`);
		}

		//if (password.value == password.value.toUpperCase()) {
		if (ucase.test(password.value) == true) {
			uper.classList.remove("fas", "fa-times");
			uper.classList.add("fas", "fa-check-circle");
		} else if (ucase.test(password.value) == false) {
			uper.classList.remove("fas", "fa-check-circle");
			uper.classList.add("fas", "fa-times");
			showErrorText(password, `${getFieldNameChange(password)} must have at least 1 Uppercase`);
		}
		if (specialChars.test(password.value) == true) {
			spchar.classList.remove("fas", "fa-times");
			spchar.classList.add("fas", "fa-check-circle");
		} else if (specialChars.test(password.value) == false) {
			spchar.classList.remove("fas", "fa-check-circle");
			spchar.classList.add("fas", "fa-times");
			showErrorText(password, `${getFieldNameChange(password)} must have atleast 1 special character`);
		}
		if (num.test(password.value) == true) {
			int.classList.remove("fas", "fa-times");
			int.classList.add("fas", "fa-check-circle");
		} else if (num.test(password.value) == false) {
			int.classList.remove("fas", "fa-check-circle");
			int.classList.add("fas", "fa-times");
			showErrorText(password, `${getFieldNameChange(password)} must have 1 number`);
		}
	};
}
function showErrorText(input, message) {
	const formControl = input.parentElement;
	formControl.className = "form-group col-md-8 eye_icon";
	const div = formControl.querySelector("div");
	div.innerText = message;
}


function forgotUserValidate() {
	var userName = $("#userNameText").val();
	if (!(userName != null && userName != undefined && userName != '')) {
		$("#userNameError").css("color", "red");
		$("#userNameError").text("Username cannot be empty.");
		return;
	}
	showLoader();
	$("#Loader").addClass("modalLoader");
	var user = userName.toUpperCase();
	$("#userNameError").text("");
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: 'getProcessLoginAuth',
		traditional: true,
		cache: false,
		data: {
			userName: user
		},
		success: function(response) {
			stopLoader();
			var message = response['message'];
			var maskedMail = response['email'];
			if (message != undefined && message != '' && message != null) {
				$("#userNameError").css("color", "red");
				$("#userNameError").text(message);
			} else {
				beforeLoginOtpSend(user, maskedMail);


			}
		}
	});
}
function beforeLoginOtpSend(username, maskedMail) {
	showLoader();
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: 'getProcessLoginOtpAuth',
		traditional: true,
		cache: false,
		data: {
			userName: username,
			beforeLogin: 'Y'
		},
		success: function(response) {
			stopLoader();
			var status = response['status'];
			if (status != null && status != "" && status != undefined && status == 'success') {
				var otpInput = response['otpInput'];
				var OtpCode = response['otp'];
				localStorage.setItem("beforLoginotp", OtpCode);
				$("#otp-wrapper").html(otpInput);
				$(".userHideLabel").hide();
				$(".userValidClass").hide();
				$("#userNameText").hide();
				$("#loginOtpMsg").show();
				$("#loginOtpMsg").text("User successfully validated. An OTP has been sent to the registered email address.");
				$("#loginOtpMsg").css("color", "green");
				$("#loginOtpMsg").append("<span style='color:#0b4a99;'>" + maskedMail + "</span>");
				$("#loginOtpCode").focus();
				$("#Loader").removeClass("modalLoader");
				$(".changepassword .titlepasswordClass.row").css("margin-top", "0");
			} else {
				$("#loginOtpStatus").text("User not added email address.");
			}
		}
	});
}

function checkOtpAndEnableIp() {
	var otpField = $("#loginOtpCode").val();
	var security = localStorage['beforLoginotp'];
	if (security == btoa(otpField)) {
		$("#loginOtpMsg").hide();
		$("#loginOtpCode").hide();
		$("#otpLabelId").hide();
		$("#loginOtpStatus").hide();
		$("#checkOtpAndEnableId").hide();
		$("#otpStatusMsg").css("color", "green");
		$("#otpStatusMsg").text("OTP verified successfully. Please provide the details below")
		$("#oldpassword").prop("readonly", false);
		$("#newPassword").prop("readonly", false);
		$("#confirmPassword").prop("readonly", false);


	} else {
		$("#otpStatusMsg").css("color", "red");
		if (!(otpField != null && otpField != undefined && otpField != '')) {
			$("#otpStatusMsg").text("OTP cannot be empty.");
		} else {
			$("#otpStatusMsg").text("The OTP entered does not match");
		}
	}
}







function showRegisterForm() {
	var username = $('#ssUsername').val();
	showLoader();
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: 'checkForRole',
		traditional: true,
		cache: false,
		async: false,
		data: {
			userName: username,
		},
		success: function(response) {
			stopLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				var status = response['status'];
				var formData = response['formData'];
				if (status) {
					$("#dialog").html(formData);
					$("#dialog").dialog({
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Register User'),
						modal: true,
						width: '850px',
						height: 'auto',
						fluid: true,
						buttons: [{
							text: (labelObject['Register'] != null ? labelObject['Register'] : 'Register'),
							click: function() {
								var userDetails = {};
								var allDone = false;
								$('#formForUserRegisterId input, #formForUserRegisterId select').each(function() {
									var labelName = $(this).data('label');
									var key = $(this).data('colname');
									var value = $(this).val();
									if (!value) {

										if ($(this).next().hasClass('visionMasterDetailForm_err')) {
											allDone = true;
											$(this).next('.visionMasterDetailForm_err').text(labelName + ' cannot be empty');
										}
										if ($(this).next().hasClass('visionMasterDetailFormddw')) {
											allDone = true;
											$(this).parent().next('.visionMasterDetailForm_err').text(labelName + ' cannot be empty');
										}
									}
									else {
										userDetails[key] = value;
									}
								});
								if (allDone) return
								else {
									createUser(userDetails);
									$(this).html("");
									$(this).dialog("close");
									$(this).dialog("destroy");
								}
								try {
								} catch (e) {
								}
							}
						}, {
							text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
							click: function() {
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
								//   fetchTabData(tableName);
								try {
								} catch (e) {
								}
							}
						}
						],
						open: function() {

						},
						beforeClose: function(event, ui) {

						}
					});


				} else {
					var modalObj = {
						title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
						body: `<div style='display: flex;'><span><img src='images/SignUp.png' class='themeModeDark' style='width:22px;'></span><span style='font-weight:700;margin-left: 8px;'><h6>${formData}</h6></span></div>`,
					};
					var buttonArray = [
						{
							text: 'Ok',
							click: function() {

							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("dataDxpSplitterValue", modalObj);
					$(".modal-dialog").addClass("modal-xs");
				}
			}
		}
	});
}

function isUserAvailable() {
	showLoader();
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'JSON',
		cache: false,
		url: "checkUserAvailability",
		data: {
			USER_NAME: $("#USER_NAME").val()
		},
		success: function(response) {
			stopLoader();
			var status = response['status'];
			if (parseInt(status) > 0) {


				var modalObj = {
					title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
					body: `<div style='display: flex;'><span><img src='images/SignUp.png' class='themeModeDark' style='width:22px;'></span><span style='font-weight:700;margin-left: 8px;'><h6>User ${$("#USER_NAME").val()} Already exists, please choose other username</h6></span></div>`,
				};
				var buttonArray = [
					{
						text: 'Ok',
						click: function() {

						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
				$(".modal-dialog").addClass("modal-xs");
				$("#USER_NAME").val('');
			} else {
				$('#updateUserInfo').removeAttr('disabled');

			}

		},
		error: function(e) {
			sessionTimeout(e);
		}

	});
}

function createUser(userDetails) {
	showLoader();
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'JSON',
		cache: false,
		url: "createUser",
		data: {
			griddata: JSON.stringify(userDetails),
		},
		success: function(response) {
			stopLoader();
			var status = response['message'];
			var modalObj = {
				title: labelObject['Message'] != null ? labelObject['Message'] : 'Message',
				body: `<div style='display: flex;'><span><img src='images/SignUp.png' class='themeModeDark' style='width:22px;'></span><span style='font-weight:700;margin-left: 8px;'><h6>${status}</h6></span></div>`,
			};
			var buttonArray = [
				{
					text: 'Ok',
					click: function() {

					},
					isCloseButton: true
				}
			];
			modalObj['buttons'] = buttonArray;
			createModal("dataDxpSplitterValue", modalObj);
			$(".modal-dialog").addClass("modal-xs");
		},
		error: function(e) {
			sessionTimeout(e);
		}
	});


}




function showModelPopup(obj) {
	$('#' + obj.id).html(obj.body);
	$('#' + obj.id).dialog({
		modal: true,
		title: (labelObject[obj.title] != null ? labelObject[obj.title] : obj.title),
		height: obj.height,
		minHeight: obj.minHeight,
		minWidth: obj.minWidth,
		maxWidth: obj.maxWidth,
		dialogClass: obj.class + ' dailogclose',
		fluid: true,
		//        buttons: [{
		//                text: (labelObject[obj.BTNText] != null ? labelObject[obj.BTNText] : obj.BTNText),
		//                click: function () {
		//                    $(this).html("");
		//                    $(this).dialog("close");
		//                    $(this).dialog("destroy");
		//                },
		//                class: obj.btnClss
		//            }]
	});
}
function updatePassFormBeforeLogin() {

	var result = '<div class="changepassword" id="changepassword">'
		+ '<div class="titlepasswordClass row ">'
		+ '<div class="col-12" id="loginOtpMsg" style="display:none"></div>'
		+ '<div class="col-12"id="otpStatusMsg"></div>'
		+ '<div id=\"otp-wrapper\"></div>'

		+ '<div class="col-md-4 userHideLabel"><label  data-error="wrong" data-success="right" for="form34">User Name<sup style="color: red">*</sup></label></div>'
		+ '<div class="col-md-8 userHideLabel form-group"><div class=\"user-wrapper\"><input autocomplete=\"off\" type=\"text\" name=\"userNameText\" id=\"userNameText\" class="userNameText form-control " title=\"\"\"><button class="userValidClass btn btn-primary" onclick="forgotUserValidate()">Validate</button></div>'
		+ "<div id='userNameError'></div>"
		+ '</div>'
		//            + '<div class="col-md-4"><label data-error="wrong" data-success="right" for="form34">Old Password<sup style="color: red">*</sup></label></div>'
		////            + '<div class="col-md-8 form-group eye_icon"><input autocomplete=\"off\" type=\"password\" name=\"password\" id=\"oldpassword\" class="oldPassword form-control" onblur=\"genericpasswordvalidation(id)\" placeholder=\"\" title=\"\" data-pswdregex=\"(.){8,15}:::[0-9]{1,}:::[a-z]{1,}:::[A-Z]{1,}:::([!,%,&amp;,@,#,$,^,*,?,_,~]{1,}):::(?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\\s).{8,15}:::\" data-pswddesc=\"Password must contain minimum of 8 characters:::Password must contain one number(0-9):::Password must contain one lowercase letter (a-z):::Password must contain one Uppercase letter (A-Z):::Password must contain one Special Character:::Enter Password As Specified:::\"> <i class="fa fa-eye-slash" id="eyeshowhide" onclick="showPassword()" aria-hidden="true"></i>'
		//            + '<div class="col-md-8 form-group eye_icon"><input autocomplete=\"off\" type=\"password\" name=\"password\" id=\"oldpassword\" class="oldPassword form-control" placeholder=\"\" title=\"\"  > <i class="fa fa-eye-slash" id="eyeshowhide" onclick="showPassword()" aria-hidden="true"></i>'
		//            + "<span id=\"error_old_password\" class=\"all_errors\"></span>"
		//            + "<div id='dis_oldpassword' class='all_errors'></div>"
		//            + '</div>'
		+ '<div class="col-md-4">'
		+ "<label data-error='wrong' data-success='right' for='form34'>New Password<sup style='color: red'>*</sup></label>"
		+ '</div>'
		+ '<div class="col-md-8 form-group eye_icon">'
		+ "<input autocomplete='off' type='password' name='password' id='newPassword' class='newPassword form-control' onblur=\"genericpasswordvalidation(id)\" placeholder='' title='EX:New Password' data-pswdregex=\"(.){8,15}:::[0-9]{1,}:::[a-z]{1,}:::[A-Z]{1,}:::([!,%,&amp;,@,#,$,^,*,?,_,~]{1,}):::(?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\\s).{8,15}:::\" data-pswddesc=\"Password must contain minimum of 8 characters:::Password must contain one number(0-9):::Password must contain one lowercase letter (a-z):::Password must contain one Uppercase letter (A-Z):::Password must contain one Special Character:::Enter Password As Specified:::\">"
		//+"<i class='fa fa-eye-slash' id='neweyeshowhide' onclick='showNewPassword()' aria-hidden='true'></i>"
		+ "<span id=\"error_pwd\" class=\"all_errors\"></span>"
		+ "<div id='dis_newPassword' class='all_errors'></div>"
		+ '<div class="passwordDecription" style="display:none">'
		+ '<div class="gridd"><span id="8char" class="fa fa-times"></span><span class="text-body">min length 8</span><span id="caps" class="fa fa-times"></span>'
		+ '<span class="text-body">Uppercase</span><span id="spchar" class="fa fa-times"></span><span class="text-body">Special Character</span><span id="int" class="fa fa-times"></span><span class="text-body">number</span>'
		+ '</div></div>'
		+ '</div>'

		+ '<div class="col-md-4">'
		+ "<label data-error='wrong' data-success='right' for='form34'>Confirm Password<sup  style='color: red'>*</sup></label>"
		+ '</div>'
		+ '<div class="col-md-8 form-group eye_icon">'
		+ "<input autocomplete=\"off\" type=\"password\" name=\"password\" id=\"confirmPassword\" class='confirmPassword form-control' onblur=\"return checkPasswordMatchReType()\" placeholder=\"\" title=\"\" data-pswdregex=\"(.){7,15}:::[0-9]{1,}:::[a-z]{1,}:::[A-Z]{1,}:::([!,%,&amp;,@,#,$,^,*,?,_,~]{1,}):::(?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\\s).{8,15}:::\" data-pswddesc=\"Password must contain minimum of 8 characters:::Password must contain one number(0-9):::Password must contain one lowercase letter (a-z):::Password must contain one Uppercase letter (A-Z):::Password must contain one Special Character:::Enter Password As Specified:::\">"
		//+"<i class='fa fa-eye-slash' id='confirmeyeshowhide' onclick='showConfirmPassword()' aria-hidden='true'></i>"
		+ "<span id=\"error_password2\" class=\"all_errors\"></span>"
		+ "<div id='dis_confirmPassword' class='all_errors'></div>"
		+ '</div>'
		+ "<div class=\"visionChangePasswordData visionErrorCells col-12\" >"
		+ "<span id=\"error_password2\" class=\"all_errors\"></span>"
		+ "<div id=\"dis_password2\" class=\"all_errors\"></div></div>"
		+ "<div class=\"visionChangePasswordData visionErrorCells col-12\"> <input type=\"text\" style=\"display: none\" value=\"\" id=\"pers_Id\"></div>"
		+ "</div>"
		+ '</div>';
	var modalObj = {
		title: labelObject['Message'] != null ? labelObject['Message'] : 'Forgot Password',
		body: result
	};
	var buttonArray = [
		{
			text: 'Apply Changes',
			click: function() {
				checkpasswordchange('N')

			},
			isCloseButton: false
		},
		{
			text: 'Cancel',
			click: function() {
			},
			isCloseButton: true
		}
	];
	modalObj['buttons'] = buttonArray;
	createModal("dataDxpSplitterValue", modalObj);
	$(".modal-backdrop").show();
	$(".modal-dialog").addClass("modal-md");
	$(".dataDxpSplitterValue").addClass("updatepasswordClass");
	//    $(".modal-header").append('<img class="logo" src="images/PilogCloudRedBlue.gif">');
	passDescriptionShow();
	removeCopyPasteEvent();
	$("#oldpassword").prop("readonly", true);
	$("#newPassword").prop("readonly", true);
	$("#confirmPassword").prop("readonly", true);
	//    var objstrValue = $('#userProfileImgDiv').attr('objstr');
	//    var inputBox = $('#rsUsername').val(objstrValue);
	//    inputBox.val(objstrValue);
	try {
		var rsUsername = $('#rsUsername').val();
		if (rsUsername != null && rsUsername != '' && rsUsername != undefined && rsUsername != 'undefined') {
			$("#userNameText").val(rsUsername);
		} else {
			$("#userNameText").val('');
		}
	} catch (e) {
		$("#userNameText").val('');
	}
	$("#userNameText").prop("readonly", false);

}


function removeCopyPasteEvent() {
    const inputFields = document.querySelectorAll('#newPassword, #confirmPassword');

    inputFields.forEach((inputField) => {
        ['copy', 'cut', 'paste'].forEach((event) => {
            inputField.addEventListener(event, (e) => e.preventDefault());
        });
    });
}


function err_msg(id, msg) {
    $(id).fadeIn(1000).html(msg);
}
function getFieldNameChange(input) {
    return input.id.charAt(0).toUpperCase() + input.id.slice(1);
}












