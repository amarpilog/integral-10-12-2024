
function getform(tittle) {
	console.log();
	$.ajax({
		type: "POST",
		url: "getinfo",
		cache: false,
		dataType: 'json',
		async: false,
		data: {
			tittle: tittle
		},
		success: function(response) {
			if (response != null && response != '') {
				var result = response['result'];
				$("#scbscriptionid").html("");
				$("#scbscriptionid").append("<div id='headertittleId' class='headertittleclass'>");
				$("#headertittleId").append("<div id='companyid' class='companyclass'>");
				$("#headertittleId").append("<div id='subscriptiontittleId' class='subscriptiontittleclass'>");
				tittle = tittle.replace("_", " ");
				$("#subscriptiontittleId").append("<h3>" + tittle + "<h3>");
				$("#companyid").append("<img src='images/PiLog_Logo_New.png'  class='imageLogoClass visionVisualChartBoxSelected'>");
				$("#scbscriptionid").append(result);
				var data = ["PRODUCT", "SERVICE", "CUSTOMER", "VENDOR"];
				$("#jqxdomain").jqxDropDownList({ source: data, placeHolder: "Choose Domain", width: '314px', height: '40px', dropDownHeight: 120, checkboxes: true });
				var erpdata = ["Oracle", "Oracle EBS", "D365", "Others"];
				$("#jqxerp").jqxDropDownList({ source: erpdata, placeHolder: "Choose ERP", width: '649px', height: '40px', dropDownHeight: 120, checkboxes: true });
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}

	});
}

function getconfig() {
	$.ajax({
		type: "POST",
		url: "getinfo",
		cache: false,
		dataType: 'html',
		async: false,
		data: {
		},
		success: function(response) {
			if (response != null && response != '') {
				$(".container").html("");
				$(".container").append(response);
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}

	});
}
function cartPaymentProcess(tittle, event) {
	event.preventDefault();
	var requireFlag = false;
	$("#checkedItemsLog").html("");
	$("#ERPcheckedItems").html("");
	var invalidFields = document.querySelectorAll(':invalid');
	//    if (invalidFields.length > 0) {
	//        throw new error("invalid");
	//    }
	//     var items = $("#jqxdomain").jqxDropDownList('getCheckedItems');
	//    if (tittle == 'Basic') {   
	//        if (items.length > 1 || items.length == "") {
	//            $("#checkedItemsLog").html("Choose only One Domain");
	//            event.preventDefault();
	//            throw new error("invalid");
	//        }
	//    } else if (tittle == 'Professional') {
	//        if (items.length == "" || items.length == 0) {
	//            $("#checkedItemsLog").append("Choose any Domain.");
	//            event.preventDefault();
	//            throw new error("invalid");
	//        } else if (items.length > 3){
	//            $("#checkedItemsLog").append("Choose any Three Domain(s).");
	//            event.preventDefault();
	//            throw new error("invalid");
	//        }
	//    } else if (tittle == 'Enterprise_Model') {
	//        if (items.length == "") {
	//            $("#checkedItemsLog").append("Choose any One Domain");
	//            event.preventDefault();
	//            throw new error("invalid");
	//        }
	//    }
	//    var erpitems = $("#jqxerp").jqxDropDownList('getCheckedItems');
	//    if (tittle == 'Basic') {
	//        if (erpitems.length > 1 || erpitems.length == "") {
	//            $("#ERPcheckedItems").html("**Choose any One ERP.");
	//            event.preventDefault();
	//            throw new error("invalid");
	//        }
	//    } else if (tittle == 'Professional') {
	//        if (erpitems.length == 0 || erpitems.length == "") {
	//            $("#ERPcheckedItems").append("**Choose any ERP(s).");
	//            event.preventDefault();
	//            throw new error("invalid");
	//        } else if (erpitems.length > 3 ) {
	//            $("#ERPcheckedItems").append("**Choose any Two ERP(s).");
	//            event.preventDefault();
	//            throw new error("invalid");
	//        }
	//    } else if (tittle == 'Enterprise_Model') {
	//        if (erpitems.length == "") {
	//            $("#ERPcheckedItems").append("**Choose any ERP(s).");
	//            event.preventDefault();
	//            throw new error("invalid");
	//        }
	//    }
	var domain = $("#jqxdomain").val();
	var erp = $("#jqxerp").val();
	var companyname = $("#ORGN_NAME").val();
	event.preventDefault();
	paymentsRedirect(tittle);
	//  $.ajax({
	//        type: "POST",
	//        url: "updateInfodata",
	//        cache: false,
	//        dataType: 'json',
	//        async: false,
	//        data: {
	//            orgname: $("#ORGN_NAME").val(),
	//            city: $("#CUSTOMER_CITY").val(),
	//            state: $("#CUSTOMER_STATE").val(),
	//            country: $("#CUSTOMER_COUNTRY").val(),
	//            name: $("#CONTACT_NAME").val(),
	//            phoneno: $("#CONTACT_PHONE_NO").val(),
	//            mailId: $("#CONTACT_MAIL_ID").val(),
	//            type: tittle,
	//            domain: $("#jqxdomain").val(),
	//            erp: $("#jqxerp").val()
	//        },
	//        success: function (response) {
	//            if (response != null && response != '') {
	//                var responsemsg = response['message'];
	//                $("#scbscriptionid").html("");
	//                $("#scbscriptionid").append("<div id='headertittleId' class='headertittleclass'>");
	//                $("#headertittleId").append("<div id='companyid' class='companyclass'>");
	//                $("#headertittleId").append("<div id='subscriptiontittleId' class='subscriptiontittleclass'>");
	//                tittle = tittle.replace("_", " ");
	//                $("#subscriptiontittleId").append("<h3>" + tittle + "<h3>");
	//                $("#companyid").append("<img src='images/PiLog_Logo_New.png'  class='imageLogoClass visionVisualChartBoxSelected'>");
	//                $("#scbscriptionid").html(responsemsg);
	//                $('.firstinitialStage').addClass('currentStage');
	//            }
	//        },
	//        error: function (e) {
	//            console.log(e);
	//            sessionTimeout(e);
	//            stopLoader();
	//        }
	//
	//    });
}
function updateformdata(tittle) {
	$('#paymentBodyFormPage .inputPayment, select, #jqxdomain, #jqxerp').map(function() {
		let val = $(this).val();
		if (val !== '') {
			$(this).css('border', '2px solid #c7c7c7');
		} else {
			$(this).css('border', '2px solid red');
		}
	});
	let checkedCon = $('#paymentFormCheck').is(':checked'); //som
	if (checkedCon == true) {
		showLoader(); //som
		var obj = {};
		$("#paymentBodyFormPage .inputPayment,select").each(function(i, v) {
			console.log();
			obj[v.name] = v.value;
		});
		obj.Domain = $('#jqxdomain').val();
		obj.ERP = $('#jqxerp').val();
		$("#formPaymentInfo").val(JSON.stringify(obj));
		$.ajax({
			type: "GET",
			url: "updateInfodata",
			cache: false,
			dataType: 'json',
			async: false,
			data: {
				orgname: $("#ORGN_NAME").val(),
				city: $("#CUSTOMER_CITY").val(),
				state: $("#CUSTOMER_STATE").val(),
				country: $("#CUSTOMER_COUNTRY").val(),
				name: $("#CONTACT_NAME").val(),
				phoneno: $("#CONTACT_PHONE_NO").val(),
				mailId: $("#CONTACT_MAIL_ID").val(),
				type: tittle,
				domain: $("#DOMAIN").val(),
				erp: $("#ERP").val()
			},
			success: function(response) {
				stopLoader(); //som
				if (response != null && response != '') {
					var responsemsg = response['message'];
					$("#scbscriptionid").html("");
					$("#scbscriptionid").append("<div id='headertittleId' class='headertittleclass'>");
					$("#headertittleId").append("<div id='companyid' class='companyclass'>");
					$("#headertittleId").append("<div id='subscriptiontittleId' class='subscriptiontittleclass'>");
					tittle = tittle.replace("_", " ");
					$("#subscriptiontittleId").append("<h3>" + tittle + "<h3>");
					$("#companyid").append("<img src='images/PiLog_Logo_New.png'  class='imageLogoClass visionVisualChartBoxSelected'>");
					$("#scbscriptionid").html(responsemsg);
					$('.firstinitialStage').addClass('currentStage');
				}
			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}

		});
	} else {
		stopLoader();
	}
}
function getprivious() {
	$('#subcriptionAddOnsAndDiscount').remove();
	$('#paymentDirectBodyFormPage').show();
	$('#scbscriptionid .RightContainer .downBtnField').show();
	$('#paymentBodyFormPage').show();
}

function expandfeature(cond) {

	if ($("[data-target='#accordion']").hasClass("collapsed")) {
		$("[data-target='#accordion']").removeClass("collapsed");
		$(".collapse").addClass("show");
		$("#expandImageId").attr("src", "images/Minimize_Icon.png");
	} else {
		$("[data-target='#accordion']").addClass("collapsed");
		$(".collapse").removeClass(("show"));
		$("#expandImageId").attr("src", "images/plus-solid.svg");
	}

}
function getverificationcode(number, tittle) {
	var emailId = $("#CONTACT_MAIL_ID").val();
	showLoader();
	$.ajax({
		type: "POST",
		url: "getverificationcode",
		cache: false,
		dataType: 'json',
		async: true,
		data: {
			emailId: emailId,
			number: number,
			tittle: tittle
		},
		success: function(response) {
			stopLoader();
			if (response != null && response != '') {
				var responsemsg = response['message'];
				$("#scbscriptionid").html("");
				$("#scbscriptionid").append("<div id='headertittleId' class='headertittleclass'>");
				$("#headertittleId").append("<div id='companyid' class='companyclass'>");
				$("#headertittleId").append("<div id='subscriptiontittleId' class='subscriptiontittleclass'>");
				tittle = tittle.replace("_", " ");
				$("#subscriptiontittleId").append("<h3>" + tittle + "<h3>");
				$("#companyid").append("<img src='images/PiLog_Logo_New.png'  class='imageLogoClass visionVisualChartBoxSelected'>");
				$("#scbscriptionid").html(responsemsg);
				$('.firstinitialStage').addClass('currentStage');
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}

	});
}

function getpayment(event, otpnumber, idnumber, tittle) {
	event.preventDefault();
	//    var otp = $("#VERIFICATION_ID").val();
	//    if (otp == otpnumber) {
	//        paymentsRedirect();
	getaddOnPkg(event, otpnumber, idnumber, tittle);
	//    } else {
	//        $("#errorId").html("Otp is not match");
	//    }


}

function getaddOnPkg(event, otpnumber, idnumber, tittle) {
	$.ajax({
		datatype: "json",
		type: "POST",
		url: "addOnpackage",
		data: {
			idnumber: idnumber
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				var responsemsg = response['result'];
				$("#scbscriptionid").html("");
				$("#scbscriptionid").append("<div id='headertittleId' class='headertittleclass'>");
				$("#headertittleId").append("<div id='companyid' class='companyclass'>");
				$("#headertittleId").append("<div id='subscriptiontittleId' class='subscriptiontittleclass'>");
				tittle = tittle.replace("_", " ");
				$("#subscriptiontittleId").append("<h3>" + tittle + "<h3>");
				$("#companyid").append("<img src='images/PiLog_Logo_New.png'  class='imageLogoClass visionVisualChartBoxSelected'>");
				$("#scbscriptionid").html(responsemsg);
				$('.firstinitialStage').removeClass('currentStage');
				$('.firstinitialStage').addClass('successStage');
				$('.step1Line').addClass('successStage');
				$('.firstinitialStage').append('<span class=\'successCheckMark\'><i class=\"fa fa-check\" aria-hidden=\"true\"></i></span>');
			}
		},
		error: function(e) {
			stopLoader();
			alert('Error: ' + e);
		}
	});
}

function changeEmail(number, title, toEmail) {
	//    event.preventDefault();
	var message = "  <div class=\"container integralStepFormDiv\">\n"
		+ "        <div class=\"integralStepForm\">\n"
		+ "            <section class=\"step-indicator\">\n"
		+ "                <div class=\"step step1 active\">\n"
		+ "                    <div class=\"step-icon\"></div>\n"
		+ "                    <p>Company Information</p>\n"
		+ "                </div>\n"
		+ "                <div class=\"indicator-line\"></div>\n"
		+ "                <div class=\"step step3\">\n"
		+ "                    <div class=\"step-icon\"></div>\n"
		+ "                    <p>Payment Status</p>\n"
		+ "                </div>\n"
		+ "            </section>\n"
		+ "<div id='verificationId' class = 'verificationclass'>"
		+ "<span class='configurationtittle'>Email verification </span>"
		+ "<div id='mailconfirmationId' class = 'mailconfirmationclass'>"
		+ " <div class=\"form-group col-md-12\">\n"
		+ "            <div class=\"form-group col-md-12 inputBoxMainDiv\">\n"
		+ "                <span>Email Id</span>"
		+ "                <input type=\"text\" value =" + toEmail + " class=\"form-control\" id=\"CONTACT_MAIL_ID\">\n"
		+ "            </div>\n"
		+ "            </div>\n"
		+ "<button class=\"btn btn-primary\" id=\"btn\"  onclick=getverificationcode('" + number + "','" + title + "');>Send verification</button>\n";
	message += "</div>";
	message += "</div>";
	message += "</div>";
	message += "</div>";
	$("#scbscriptionid").html("");
	$("#scbscriptionid").append("<div id='headertittleId' class='headertittleclass'>");
	$("#headertittleId").append("<div id='companyid' class='companyclass'>");
	$("#headertittleId").append("<div id='subscriptiontittleId' class='subscriptiontittleclass'>");
	$("#companyid").append("<img src='images/PiLog_Logo_New.png'  class='imageLogoClass visionVisualChartBoxSelected'>");
	$("#scbscriptionid").html(message);
}

//function cartPaymentProcess(event, totalamount, userid) {
//    event.preventDefault();
//    var ssUsername = $("#ssUsername").val();
//    var selectedRole = "";
//    var subScriptionObjArray = [];
//
//    var subScriptionObj = {};
////     subScriptionObj['apiId'] = apiId;
//    subScriptionObj['volume'] = 5;
//    subScriptionObj['usersCount'] = 10;
//    subScriptionObj['price'] = 20; 
//    subScriptionObj['duration'] = 10;
//    subScriptionObjArray.push(subScriptionObj);
//    if (subScriptionObjArray != null && subScriptionObjArray.length != 0) {
//        var options = {};
//        $.ajax({
//            datatype: "json",
//            type: "POST",
//            url: "processCheckout",
//            data: {
//                subScriptionObjArray: JSON.stringify(subScriptionObjArray),
//                totalamount: totalamount,
//                userid: userid
//            },
//            traditional: true,
//            cache: false,
//            success: function (response) {
//                stopLoader();
//                if (response != null && !jQuery.isEmptyObject(response)) {
//                    options = response;
//                    options.handler = function (successResponse) {
//                        paymentResponse(successResponse, subScriptionObjArray, response, "Y", selectedRole);
//                    };
//                    options.notes = {
//                        "address": "note value"
//                    };
//                    options.theme = {
//                        "color": "#9932CC"
//                    };
////                        var propay = new Razorpay(options);
//                    const propay = new window.Razorpay(options);
//                    propay.on('payment.failed', function (failResponse) {
//                        paymentResponse(failResponse, subScriptionObjArray, response, "N", selectedRole);
//                    });
//
//                    propay.open();
//                }
//            },
//            error: function (e) {
//                stopLoader();
//                alert('Error: ' + e);
//            }
//        });
//    } else {
//        var modalObj = {
//            title: 'Message',
//            body: 'Please select atleast one api to checkout.'
//        };
//        var buttonArray = [
//            {
//                text: 'Ok',
//                isCloseButton: true,
//            }
//        ];
//        modalObj['buttons'] = buttonArray;
//        createModal("modalDailogDiv1", modalObj);
//    }
//
//}

function updateprice($this, featureName, subfeaturename) {
	var ids = $(event.currentTarget).attr('id');
	var activeflag = false;
	if ($($this).is(":checked")) {
		activeflag = "Y"
	} else {
		activeflag = "N";
	}
	$.ajax({
		datatype: "json",
		type: "POST",
		url: "updatepricedata",
		data: {
			featureName: featureName,
			subfeaturename: subfeaturename,
			activeflag: activeflag
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				$("#totalamountid").val("");
				var totatamount = response['totalamount'];
				$("#totalamountid").html(totatamount);
			}
		},
		error: function(e) {
			stopLoader();
			alert('Error: ' + e);
		}
	});
}

function getCountryCode(event) {
	var code = $("#CUSTOMER_COUNTRY").val();
	var dialcode = $('#CUSTOMER_COUNTRY').attr('data-dialcode');
	$('#dialCode').val(dialcode);
	let country = $("#CUSTOMER_COUNTRY").val().toUpperCase();
	/*if (country == 'INDIA') {
		$('#CUSTOMER_CURRENCY').val('INR');
	} else {
		$('#CUSTOMER_CURRENCY').val('USD');
	}*/
	$.ajax({
		datatype: "html",
		type: "POST",
		url: "getstate",
		data: {
			country: code
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				$("#CUSTOMER_STATE").html(response);
				$("#CUSTOMER_CITY").html('<option value="">Select City</option>');
			}
		},
		error: function(e) {
			stopLoader();
			alert('Error: ' + e);
		}
	});
}

function imageMouseHover(image) {
	var imgString = image;
	$("#pricingCustomeTable").empty();
	$("#pricingCustomeTable").show();
	$("#pricingCustomeTable").append(imgString);
}
function createIDXPCalendarEventForm(calendarId) {
	showLoader();
	$.ajax({
		type: "POST",
		url: 'calEventForm',
		dataType: 'html',
		data: {
			calendarId: calendarId
		},
		traditional: true,
		cache: false,
		success: function(result) {
			stopLoader();
			if (result != null && result != '') {
				var eventFormObj = JSON.parse(result);
				if (eventFormObj != null && !jQuery.isEmptyObject(eventFormObj)) {

					$("#dialog").html(eventFormObj['formStr']);
					$("#dialog").dialog({
						title: (labelObject['Create Event'] != null ? labelObject['Create Event'] : 'Create Event'),
						modal: true,
						// width: "auto",
						width: 600,
						height: 'auto',
						maxHeight: 550,
						fluid: true,
						buttons: [
							{
								text: (labelObject['Create'] != null ? labelObject['Create'] : 'Create'),
								"class": 'dialogyes',
								click: function() {
									var errorCount = 0;
									$('#calendarCreateEventsTable :input').each(function() {
										var id = $(this).attr("id");
										var inputVal = $("#" + id).val();
										var mandatoryVal = $(this).attr("data-mandatory");
										if (mandatoryVal != null && mandatoryVal == 'M') {
											if (inputVal != null && inputVal != '') {
												$("#dis" + id).hide();
											} else {
												errorCount++;
												var errid = "#dis" + id;
												var msg = "Should not be empty";
												errorMessage(errid, msg);
											}

										}
									});
									if (errorCount == 0) {
										$(".all_errors").hide();
										saveGridCalendarData('dialog', eventFormObj, calendarId);
									}

								}
							}
						],
						open: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
							var datePickersCols = eventFormObj['datePickerIds'];
							if (datePickersCols != null && datePickersCols != '') {
								var datePickersColsArray = datePickersCols.split(",");
								for (var i = 0; i < datePickersColsArray.length; i++) {
									if (datePickersColsArray[i] != null && datePickersColsArray[i] != "") {
										$("#" + datePickersColsArray[i]).datepicker({
											changeMonth: true,
											changeYear: true,
											dateFormat: "yy-mm-dd",
											showOn: "button",
											buttonImage: 'images/date_picker_icon.png',
											buttonImageOnly: true
										});
									}
								}
							}
							$("#ui-datepicker-div").addClass("ui-datepickerReports");
							var timePickersCols = eventFormObj['timePickerIds'];
							if (timePickersCols != null && timePickersCols != '') {
								var timePickersColsArray = timePickersCols.split(",");
								for (var i = 0; i < timePickersColsArray.length; i++) {
									if (timePickersColsArray[i] != null && timePickersColsArray[i] != "") {
										$('#' + timePickersColsArray[i]).timepicker({
											timeFormat: 'h:mm p',
											interval: 5,
											startTime: new Date(),
											dynamic: false,
											dropdown: true,
											scrollbar: true,
											//                    setTime:new Date()
										});
										$('#' + timePickersColsArray[i]).timepicker('setTime', new Date());
									}
								}
							}



						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});
				}

			}

			stopLoader();
		},
		error: function(e) {
			sessionTimeout(e);
			stopLoader();
		}


	});
}
function saveEventGridCalendarData(parentDailogId, eventFormObj, calendarId) {
	showLoader();
	var calendarEventFormData = {};
	$("#calendarCreateEventsTable :input").each(function() {
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

			calendarEventFormData[textid] = textval;
		}

		if (textid != null && textid.lastIndexOf("HIDDEN") > -1) {
			var columnNames = $("#" + textid).val();
			var columnsArray = columnNames.split(",");
			var hiddenIds = textid.split("HIDDEN_");
			var hiddenVal = $("#" + hiddenIds[1]).val();
			for (var i = 0; i < columnsArray.length; i++) {
				calendarEventFormData[columnsArray[i]] = hiddenVal;
			}

		}


	});
	$.ajax({
		url: "saveEventCalGridData",
		traditional: true,
		type: "POST",
		dataType: 'html',
		cache: false,
		traditional: true,
		cache: false,
		data: {
			paramsData: JSON.stringify(calendarEventFormData),
			tableName: "O_RECORD_CAL_EVENTS",
			calendarId: "CRM_CALENDAR_EVENT"
		},
		success: function(response) {
			stopLoader();
			if (response != null && response != '') {
				var responseObj = JSON.parse(response);
				if (responseObj != null && !jQuery.isEmptyObject(responseObj)) {
					$("#dialog1").html(responseObj['message']);
					$("#dialog1").dialog({
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
								if (responseObj['messageFlag']) {
									$("#" + parentDailogId).html("");
									$("#" + parentDailogId).dialog("close");
									$("#" + parentDailogId).dialog("destroy");
									try {
										refreshCalendarData(calendarId);
									} catch (e) {
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
				}
			}


		},
		error: function(e) {
			sessionTimeout(e);
			stopLoader();
		}


	});
}
function checkFormValidation() {
	let checkId = event.currentTarget.id;
	$('#paymentBodyFormPage .inputPayment, select:visible, #jqxdomain, #jqxerp').not('#' + checkId).map(function() {
		let checkedCon = $('#paymentFormCheck').is(':checked');  //som
		if (checkedCon == true) {
			let val = $(this).val();
			if (val !== '') {
				$('#' + checkId).prop('checked', true);
				$(this).css('border', '1px solid #c7c7c7');
			} else {
				$(this).css('border', '1px solid red');
				$('#' + checkId).prop('checked', false);
			}
		}
	});
}
function paymentsRedirect(title) {
	var obj = {};
	let arrstr = '';
	$('#recommendFeatures .chechBoxPaymentRecommend').map(function(ind, val) {
		if ($(this).is(':checked')) {
			arrstr += this.parentNode.previousSibling.textContent + ', ';
		}
	});
	//som
	showLoader()    //som
	var formPaymentInfo = $("#formPaymentInfo").val();
	var requestId = $('#requestId').val();
	if (formPaymentInfo != null && formPaymentInfo != undefined && formPaymentInfo != "") {
		obj = JSON.parse(formPaymentInfo);
	}
	let totalAmount = $('#subscriptionTotalAmount').text().replaceAll(',', '');
	let totalOrginalAmount = $('#DisAnnualFee').text().replaceAll(',', '');
	let totalOrginalDisAmount = $('#subscriptionOrginalDiscountTotalAmount').text().replaceAll(',', '');
	let disCouponAmmount = $('#disCouponAmmount').text().replaceAll(',', '');
	let disCouponPercentage = $('#disCouponPercentage').text();
	let discountCode = $('#coupon-Code-Input-payment-page').val();
	obj.amount = +totalAmount;
	obj.totalOrginalAmount = +totalOrginalAmount;
	obj.totalOrginalDisAmount = +totalOrginalDisAmount;
	obj.disCouponAmmount = +disCouponAmmount;
	obj.disCouponPercentage = disCouponPercentage;
	obj.discountCode = discountCode;
	obj.addON = arrstr;
	obj['requestId'] = requestId;
	obj['Domain'] = "SmartBI";
	obj['ERP'] = "SmartBI";
	$.ajax({
		url: "paymentRequest",
		type: "POST",
		dataType: 'html',
		data: obj,
		success: function(data, textStatus, jqXHR) {
			stopLoader(); //som
			var apiUrl = data + "&output=embed";
			var iframe = "<div class='paymentResponse'><iframe width='100%' height='500' src='" + apiUrl + "' id='iFramePaymentResponse' title='Payment Response' allowfullscreen></iframe></div>";
			//              
			$("#dialog").html(iframe);
			$("#dialog").dialog({
				modal: true,
				title: (labelObject['Payment Response'] != null ? labelObject['Payment Response'] : 'Payment Response'),
				height: 'auto',
				minHeight: 'auto',
				minWidth: 500,
				maxWidth: 'auto',
				fluid: true,
				buttons: [{
					text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
					click: function() {
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

			//            var apiUrl = data + "&output=embed";
			//            var iframe = "<div class='paymentResponse'><iframe width='100%' height='500' src='" + apiUrl + "' id='iFramePaymentResponse' title='Payment Response' allowfullscreen></iframe></div>";
			//            var modalObj = {
			//                title: 'Payment Response',
			//                body: iframe
			//            };
			//            var buttonArray = [
			//                {
			//                    text: 'OK',
			//                    click: function () {
			////                        $('#iFrameVideo').attr('src', "");
			//                    },
			//                    isCloseButton: true
			//                }
			//            ];
			//            modalObj['buttons'] = buttonArray;
			//            createModal("dialog", modalObj);
			//            $(".modal-dialog").addClass("modal-xl");
			//            $(".modal-dialog").addClass("paymentReciptMainDailogClass");
			//            $('.modal-dialog').draggable({
			//                handle: ".modal-header"
			//            });
		}
	});
}



function getPaymentEmailOtp() {
	let val = $('#CONTACT_MAIL_ID').val();
	if (val != null && val != '' && val != undefined) {
		$('#CONTACT_MAIL_ID').css('border', '2px solid #c7c7c7');
		showLoader();
		$.ajax({
			dataType: 'JSON',
			type: 'POST',
			url: 'checkMailExists',
			traditional: true,
			cache: false,
			async: true,
			data: {
				emailId: val
			},
			success: function(data, textStatus, jqXHR) {
				//stopLoader();
				if (data.status) {
					$("#emailTextDiv").hide();
					$.ajax({
						dataType: 'JSON',
						type: 'POST',
						url: 'getverificationcode',
						traditional: true,
						cache: false,
						async: true,
						data: {
							emailId: val
						},
						success: function(data, textStatus, jqXHR) {
							stopLoader();
							if (data.status) {
								$('#paymentMailFeild i').remove();
								$('#otpVerificationContaniner').show();
								$('#EmailFeildForm').hide();
								$('#EmailFeildBTN').hide();
								$('#otpVerificationContaniner').html(data['str']);
								let successStyle = 'font-weight:600;position:absolute;font-style:italic;top:90%;';
								let successText = 'The OTP has been sent to your email';
								EmailError(successText, successStyle);

							} else {
								$('#otpVerificationContaniner').hide();
								$('#paymentMailFeild i').remove()
								$('#paymentMailFeild').append(data['str']);
								$('#EmailFeildForm').show();
								$('#EmailFeildBTN').show();
								$('#otpVerificationContaniner').html(data['str']);
								$("#emailTextDiv").remove();
								let errorText = "Please enter a valid email address.";
								let errorStyle = 'color:red;font-weight:600;position:absolute;font-style:italic;top:90%;right:0;'
								EmailError(errorText, errorStyle);

							}
						}
					});

				} else {
					stopLoader();
					$('#otpVerificationContaniner').hide();
					$('#paymentMailFeild i').remove()
					$('#paymentMailFeild').append(data['str']);
					$('#EmailFeildForm').show();
					$('#EmailFeildBTN').show();
					$('#otpVerificationContaniner').html(data['str']);
					$("#emailTextDiv").remove();
					let errorText = "Mail already exists,Please give new mail.";
					let errorStyle = 'color:red;font-weight:600;position:absolute;font-style:italic;top:90%;right:0;'
					EmailError(errorText, errorStyle);

				}
			}
		});
	} else {
		$('#CONTACT_MAIL_ID').css('border', '2px solid red');
	}
}


function getVerifyOTP() {
	let val = $('#paymentOTPFeild').val();
	showLoader();
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: 'getOTPVerificationcode',
		traditional: true,
		cache: false,
		async: true,
		data: {
			val: val
		},
		success: function(data, textStatus, jqXHR) {
			stopLoader();
			if (data.match) {
				$('#paymentMailFeild').append(data['str']);
				$('#otpVerificationContaniner').hide();
				$('#EmailFeildForm').show();
				$('#EmailFeildBTN').hide();
				$('#CONTACT_MAIL_ID').attr('data-attr', 'true')
				$('#emailTextDiv span').text('');
			} else {
				$('#otpVerfication i').remove();
				$('#invalidOtpClearIcon').append(data['str']);
				// $('#otpVerfication').append(data['str']);
				$('#otpVerificationContaniner').show();
				$('#EmailFeildForm').hide();
				$('#EmailFeildBTN').hide();
				$('#emailTextDiv span').text('Incorrect OTP entered, please try again with correct OTP');
			}
		}
	})
}

function getNextFeatureInfo(tittle) {
	$('#rightContainerSecondDiv .inputPayment, select').map(function() {
		let val = $(this).val();
		if (val !== null && val !== '' && val !== undefined) {
			$('#rightContainerthirdErrorDiv').hide();
			$('#rightContainerthirdErrorDiv').text('');
		} else if ($(this).attr("data-req") == "M") {
			$('#rightContainerthirdErrorDiv').show();
			$('#rightContainerthirdErrorDiv').text('Please Fill Required Details');
		}
	});
	var obj = {};
	var objdata = $('#formPaymentInfo').val();
	var requestId = $('#requestId').val();
	if (objdata != null || objdata != '' || objdata != 'null' || objdata != undefined) {
		obj = {};
	} else {
		obj = JSON.parse(objdata);
	}
	obj['requestId'] = requestId;
	let val = '';
	let name = '';
	let errCnt = 0;
	$("#rightContainerSecondDiv .inputPayment, .inputPaymentH, select:visible").each(function(i, v) {

		if (v.value != '' && v.value!='Select State') {
			obj[v.name] = v.value;
			$('#rightContainerthirdErrorDiv').hide();
			$('#rightContainerthirdErrorDiv').text('');
			$(this).css("border", "1px solid #c7c7c7");
		} else {
			errCnt = errCnt + 1;
			$('#rightContainerthirdErrorDiv').show();
			$(this).css('border', '1px solid red');
			$('#rightContainerthirdErrorDiv').text('Please Fill Required Details');
		}

	});
	$("#rightContainerFirstDiv .inputPayment").each(function(i, v) {
		//        if (v.value != '') {
		//            if (i != 0) {
		//                val += ' ' + v.value;
		//            } else {
		//                val += v.value;
		//            }
		//            name = v.name;
		//        }
		if (v.value != '') {
			obj[v.name] = v.value;
			//if (i == 0) {
			name = v.value;
			//}
		}


		if (name != '') {
			$('#rightContainerthirdErrorDiv').hide();
			$('#rightContainerthirdErrorDiv').text('');
			$(this).css("border", "1px solid #c7c7c7");
		} else if ($(this).attr("data-req") == "M") {
			errCnt = errCnt + 1;
			$('#rightContainerthirdErrorDiv').show();
			$(this).css('border', '1px solid red');
			$('#rightContainerthirdErrorDiv').text('Please Fill Required Details');
		}
		name = '';
	});
	let domainVal = $('#jqxdomain').val();
	let erpVal = $('#jqxerp').val();
	if (domainVal != '') {
		obj.Domain = domainVal;
		$('#rightContainerthirdErrorDiv').hide();
		$('#rightContainerthirdErrorDiv').text('');
	} else {
		errCnt = errCnt + 1;
		$('#rightContainerthirdErrorDiv').show();
		$('#rightContainerthirdErrorDiv').text('Please Fill Required Details');
	}
	if (erpVal != '') {
		obj.ERP = erpVal;
		$('#rightContainerthirdErrorDiv').hide();
		$('#rightContainerthirdErrorDiv').text('');
	} else {
		errCnt = errCnt + 1;
		$('#rightContainerthirdErrorDiv').show();
		$('#rightContainerthirdErrorDiv').text('Please Fill Required Details');
	}


	obj.ERP = $('#jqxerp').val();
	obj.tittle = tittle;
	if (errCnt != 0) {
		$('#paymentFormCheck').prop('checked', false);
		return;
	}

	let checkedCon = $('#paymentFormCheck').is(':checked');  //som

	if (checkedCon == true) {


		var companyName = $("input[name='billing_company']").val();
		var firstName = $("input[name='billing_name']").val();
		var lastName = $("input[name='billing_lastname']").val();
		$.ajax({
			dataType: 'json',
			type: 'POST',
			url: 'checkForCompanyAlreadyExist',
			traditional: true,
			cache: false,
			async: false,
			data: {
				companyName: companyName,
				firstName: firstName,
				lastName: lastName,
			},
			success: function(response) {
				stopLoader();
				if (response != null && !jQuery.isEmptyObject(response))
					var status = response["status"];
				if (!status) {
					$('#companyAlreadyExistId').remove();
					$("input[name='billing_company']").after("<div id='companyAlreadyExistId' class='companyAlreadyExist' style='color:red'>Company Name Already Exist.. Please give new Name</div>");
					$("input[name='billing_company']").css("color", "red");
					return;
				} else {
					var userNameStatus = response["userNameExist"];
					if (userNameStatus) {
						$("#dialog").html(response['userNameMessage']);
						$("#dialog").dialog({
							title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
							modal: true,
							height: 'auto',
							minHeight: 'auto',
							minWidth: 500,
							maxWidth: 'auto',
							fluid: true,
							buttons: [{
								text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
								click: function() {
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
						return;
					}
					$('#companyAlreadyExistId').remove();
					let emailAttr = $('#CONTACT_MAIL_ID').attr('data-attr');
					if (emailAttr == 'true') {
						$('#CONTACT_MAIL_ID').css('border', '2px solid #c7c7c7');
						showLoader();//som
						$("#formPaymentInfo").val(JSON.stringify(obj));
						$.ajax({
							datatype: "json",
							type: "POST",
							url: "addOnpackage",
							data: obj,
							traditional: true,
							cache: false,
							async: true,
							success: function(response) {
								stopLoader();
								if (response != null && !jQuery.isEmptyObject(response)) {
									var responsemsg = response['result'];
									var requestId = response['requestId'];
									$("#formPaymentInfo").val(JSON.stringify(obj));
									$("#requestId").val(requestId);
									$('#scbscriptionid .RightContainer .downBtnField').hide();
									$('#paymentBodyFormPage').hide();
									$('#first-wrapper-progress .step-icon').addClass('step-active');
									$('#first-wrapper-progress').addClass('step-active')
									if ($("#second-wrapper-progress .step-icon").hasClass("billingInfo-progress")) {
										$("#second-wrapper-progress .step-icon").removeClass("billingInfo-progress");
										$("#second-wrapper-progress .step-icon").addClass("step-billingInfoImg");
										console.log("This has class::::::::::::")
									} else {
										$("#second-wrapper-progress .step-icon").addClass("billingInfo-progress");
										$("#second-wrapper-progress .step-icon").removeClass("step-billingInfoImg");
									}
									$("#paymentDirectBodyFormPage").after(responsemsg);
									$("#paymentDirectBodyFormPage").hide();

								}
							}
						});
					} else {
						$('#CONTACT_MAIL_ID').css('border', '2px solid red');
					}
				}

			}
		});

	} else {
		$('#paymentFormCheckPopover').remove();
		$('#paymentFormCheck').append('<div id=\"paymentFormCheckPopover\" class=\"paymentFormCheckPopover\"><span>Select to agree Terms & Conditions</span></div>')
		$('#paymentFormCheckPopover').jqxPopover({
			width: 250,
			height: 30,
			position: 'top',
			selector: $('#paymentFormCheck'),
		});
		$('#paymentFormCheckPopover').jqxPopover('open');
		setTimeout(function() {
			$('#paymentFormCheckPopover').jqxPopover('close');
		}, 2000);
	}
}

var indexBasicPlan = [];
function checkChangeBasicPlan(val, id, event) {
	var title = $('#selectedTittlePlanInclude').text();
	if (id != 'jqxerp')
		if (title === 'BASIC') {
			let index = event.args.item.index;
			indexBasicPlan.push(index);
			$.each(indexBasicPlan, function(ind, val) {
				if (index != val) {
					$('#' + id).jqxDropDownList('uncheckIndex', val);
				}
			});
		} else {
			let domainCurrentVal = event.args.item.value;
			let domailVal = $('#' + id).val();
			let domainArr = domailVal.split(',');
			let domainLength = domainArr.length;
			if (!(domainLength <= 3)) {
				$('#' + id).jqxDropDownList('uncheckItem', domainCurrentVal);
			}
		}
	else {
		let index = event.args.item.index;
		indexBasicPlan.push(index);
		$.each(indexBasicPlan, function(ind, val) {
			if (index != val) {
				$('#' + id).jqxDropDownList('uncheckIndex', val);
			}
		});
	}
}

function getStateCode(event) {
	showLoader();
	var state = $('#CUSTOMER_STATE').val();
	let country = $('#CUSTOMER_COUNTRY').val();
	$.ajax({
		dataType: 'JSON',
		type: 'POST',
		url: 'getCity',
		traditional: true,
		cache: false,
		async: true,
		data: {
			country: country,
			state: state
		},
		success: function(data, textStatus, jqXHR) {
			stopLoader();
			var responce = data['City'];
			$('#CUSTOMER_CITY').html(responce);
			$('.CUSTOMER_CITY').hide();
			$('#CUSTOMER_CITY').show();
		}
	})
}

function setTotalAmount() {
	let element = event.target.closest('tr');
	let amount = +element.lastChild.children[2].textContent.replaceAll(',', '');
	let totalAmount = +$('#subscriptionTotalAmount').text().replaceAll(',', '');
	let check = event.target.checked;
	if (check == true) {
		let allAmount = totalAmount + amount;
		$('#subscriptionTotalAmount').text(allAmount.toLocaleString());
	} else {
		let allAmount = totalAmount - amount;
		$('#subscriptionTotalAmount').text(allAmount.toLocaleString());
	}
}

function setPopupMSG(type, field, image) {
	if (type == 'email') {
		var EmailHTML = "<div class=\"sentImageClass\"><img src=\"" + image + "\" alt=\"email verify\" /></div><div class=\"textImageClass\">" + field + "</div><div class='crossIconClose' onclick=\"closeVerifyBtn()\" id='closeOtpDiv'>OK</div>";
		$("#otpSentID").html(EmailHTML);
		$("#otpSentID").show();
	} else if (type == 'captch') {

	}
}

function closeVerifyBtn() {
	$("#otpSentID").hide();
}


function hideEmailShowOtp() {
	$('#otpVerificationContaniner').hide();
	$('#EmailFeildForm').show();
	$('#EmailFeildBTN').show();
	$('#emailTextDiv span').text('Please enter a valid email address.');
}


function EmailError(Text, style) {
	$("#emailTextDiv").remove();
	$("#paymentMailFeild").append("<div class='EmailTextDiv' id='emailTextDiv' style=" + style + "><span>" + Text + "</span></div>");
}

function getCountryAndCodeList() {
	return new Promise(function(resolve, reject) {
		$.ajax({
			type: 'POST',
			dataType: 'JSON',
			url: 'getCountryAndCodeList',
			traditional: true,
			cache: false,
			async: true,
			data: {},
			success: function(data, textStatus, jqXHR) {
				resolve(data['List']);
			},
			error: function(jqXHR, textStatus, errorThrown) {
				reject(errorThrown);
			}
		});
	});
}

function checkRegexValidation(event, type) {
	var val = event.target.value;
	var id = event.target.id;
	if (type == 'email') {
		var emailPattern = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
		if (emailPattern.test(val)) {
			$('#' + id).css('border', '1px solid #c7c7c7');
			$('#emailTextDiv').remove();
		} else {
			let errorText = "Please enter a valid email address.";
			let errorStyle = 'color:red;font-weight:600;position:absolute;font-style:italic;top:90%'
			EmailError(errorText, errorStyle);
			$('#' + id).css('border', '1px solid red');
		}
	} else if (type == 'mobile') {
		var mobilePattern = /^[0-9]{10}$/;
		if (mobilePattern.test(val)) {
			alert('Mobile number is valid');
			$('#' + id).css('border', '1px solid #c7c7c7');
		} else {
			$('#' + id).css('border', '1px solid red');
		}
	} else if (type == 'character') {
		var lettersOnly = /^[A-Za-z]+$/;
		if (!lettersOnly.test(val)) {
			$('#' + id).val(val.replace(/[^A-Za-z]/g, ''));
		}
	}
}


function getOtherCityCode(event) {
	let id = event.target.id;
	let val = $('#' + id).val()
	let elemet = event.target.parentNode;
	if (val == 'Other') {
		$('#' + id).remove();
		elemet.append("<input id=\"" + id + "\" class=\"" + id + " custom_input\"/>");
	}
}
function getOtherCityCode(event) {
	let id = event.target.id;
	let val = $('#' + id).val()
	let elemet = event.target.parentNode;
	if (val == 'Other') {
		$('#' + id).hide();
		$(elemet).append("<input name=\"billing_city\" class=\"" + id + " inputPayment custom_input\"/>");
	} else {
		$('.' + id).hide();
		$('#' + id).show();
	}
}

function applyDisCodeProcess(flag) {
	let elemet = event.target;
	var obj = {};
	$("#rightContainerSecondDiv .inputPayment, .inputPaymentH, select:visible").each(function(i, v) {
		if (v.value != '') {
			obj[v.name] = v.value;
		}
	});

	const country = obj['billing_country'];
	const subscriptionType = $("#CUSTOMER_SUBSCRIPTION_TYPE").val();
	let discountCode = $('#coupon-Code-Input-payment-page').val();


	showLoader();
	$.ajax({
		dataType: 'json',
		type: 'POST',
		url: 'getApplyDiscountCode',
		traditional: true,
		cache: false,
		async: true,
		data: {
			discountCode: discountCode,
			country: country,
			subscriptionType: subscriptionType,
			flag: flag
		},
		success: function(data, textStatus, jqXHR) {
			stopLoader();
			if (data['status']) {
				$('#subscriptionOrginalDiscountTotalAmount').text(data['totalAmount']);
				//         
				$('#disCouponAmmount').text(data['finalDiscountedAmount'].toLocaleString());
				$('#disCouponPercentage').text(data['discountPct']);

				let totalAmt = data['totalAmount'] - data['finalDiscountedAmount'];
				$('#subscriptionTotalAmount').text(totalAmt.toLocaleString());
				$('#disErrorMsg').hide();

				if (flag == 'N') {
					$("#remove-coupon-Code-btn").hide();
					$("#apply-coupon-Code-btn").show();
				} else {
					$("#remove-coupon-Code-btn").show();
					$("#apply-coupon-Code-btn").hide();
				}
				// $(element).prop('disabled', true);
			} else {
				$('#disErrorMsg').show();
				$('#disErrorMsg').text(data['msg']);
			}
		}
	})
}






