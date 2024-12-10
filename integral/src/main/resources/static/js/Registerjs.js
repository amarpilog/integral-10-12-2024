var oldJSON = {};

var name1 = $("#first_name").val();
var comp_code = $("#comp_code").val();
var purch_org = $("#purch_org").val();
var suppl_code = $('#vendorCode').val();
var baskettype = $('#baskettypehid').val();
if (name1 != null)
{
    oldJSON.name1 = name1.toUpperCase();
}


var specialKeys = new Array();
specialKeys.push(8); //Backspace
specialKeys.push(9); //Tab
specialKeys.push(46); //Delete
specialKeys.push(36); //Home
specialKeys.push(35); //End
specialKeys.push(37); //Left
specialKeys.push(39); //Right
specialKeys.push(32); //Space






function err_msg(id, msg) {
    $(id).fadeIn(1000).html(msg);

//    var fid = id.slice(5);   
//    $("#" + fid).focus();
}

function IsSpecialCharPD(e, ele) {

    var err = 'error_' + ele;
    var sp_readonly = $('#' + ele).attr("readonly");
    if (sp_readonly != 'readonly') {
        var keyCode = e.keyCode == 0 ? e.charCode : e.keyCode;
//        //alert(keyCode);
        if ((keyCode == 32) || (keyCode == 44) || (keyCode == 46) || (keyCode == 43) || (keyCode >= 48 && keyCode <= 57) || (keyCode >= 65 && keyCode <= 90) || (keyCode >= 97 && keyCode <= 122) || (specialKeys.indexOf(e.keyCode) != -1 && e.charCode != e.keyCode)) {

        } else {

            $("#" + err).fadeIn(500);
            $("#" + err).fadeOut(1500);
            return false;
        }
    }
}



function CheckPassword(ele) {
    var re;
    var err = 'dis_' + ele;
    var id = "#" + err;
    var str = $("#pwd").val();

    var msg = "Password must contain minimum </br> of 8 characters with one number (0-9),<br>one lowercase letter (a-z),<br>one Uppercase letter (A-Z) & <br>one Special Character!";
    if (str && str.length < 8)
    {

        err_msg(id, msg);
        return false;
    }
    re = /[0-9]/;
    if (str && !re.test(str))
    {
//        var msg = "Password must contain atleast one number (0-9)!";
        err_msg(id, msg);
        return false;
    }
    re = /[a-z]/;
    if (str && !re.test(str))
    {
//        var msg = "password must contain at least one lowercase le<br>ter (a-z)!";
        err_msg(id, msg);
        return false;
    }
    re = /[A-Z]/;
    if (str && !re.test(str))
    {
//        var msg = "password must contain at least one Uppercase letter (A-Z)!";
        err_msg(id, msg);
        return false;
    }
    re = /([!,%,&,@,#,$,^,*,?,_,~])/;
    if (str && !re.test(str))
    {
//        var msg = "password must contain at least one Special Character!";
        err_msg(id, msg);
        return false;
    }

    var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;
    var res_seq_spchar = patt_seq_spchar.test(str);

    if (res_seq_spchar == true) {
        $("#dis_pwd").html("Sequential special </br>characteres are not allowed");
        $("#dis_pwd").show();
        return false;
    }

    var patt = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\s).{8,15}$/;
    var res = patt.test(str);
    if (str && res == false)
    {
        var msg = "Enter Password As Specified";
        err_msg(id, msg);
        return false;
    }

    $("#dis_pwd").hide();


}
;



function checkPasswordMatch() {


    var pass = $("#pwd").val();
    var confirmPassword = $("#password2").val();
    if (pass == confirmPassword) {
        $("#dis_password2").html("");
    } else
    {
        $("#dis_password2").show();
        $("#dis_password2").html("Passwords do not match.");

    }

}

function resetAllValuesReg() {

//    $('.registerate').find('input:text').val('');
//    $('.dateselector-year').prop('selectedIndex', 0);
//    $('.dateselector-month').prop('selectedIndex', 0);
//    $('.dateselector-day').prop('selectedIndex', 0);
//    $('#experience_summary').val('');
//    $('.registerate').find('input:hidden').val('');
//    $('.registerate').find('input:password').val('');
//    $('.all_errors').hide();
//    $('.img-preview').hide();
//    $('#rpt_img').hide();
//    $("#crpXbtn").val("Cancel");
//    $("#crpbtn").val("Crop");
//    $("#tel_no_a").attr("disabled", "disabled");
//    $("#mob_a").attr("disabled", "disabled");
//    $("#pwd").removeAttr('disabled');
//    $("#password2").removeAttr('disabled');

    $('.visionUserRegisterInner').find('input:text').val('');
    $('.visionUserRegisterInner').find('input:text').val('');
    $('.visionAdditionalRegisterTable').find('input:text').val('');

    $('textarea').each(function () {
        $(this).val('')
    });
//    $('.registerate').find('input:text').val('');
    $('.dateselector-year').prop('selectedIndex', 0);
    $('.dateselector-month').prop('selectedIndex', 0);
    $('.dateselector-day').prop('selectedIndex', 0);
    $('#experience_summary').val('');

    $('.registerate').find('input:hidden').val('');
    $('.registerate').find('input:password').val('');
    $('.all_errors').hide();
    $('.img-preview').hide();
    $('#rpt_img').hide();
    $("#tel_no_a").attr("disabled", "disabled");
    // $("#mob_a").attr("disabled", "disabled");
    $("#pwd").removeAttr('disabled');
    $("#password2").removeAttr('disabled');
    $('.visionUserRegisterInner').find('input:hidden').val('');
    $('.visionUserRegisterInner').find('input:password').val('');
    $("#agree").prop("checked", false);
    $("#mob_a").removeAttr('disabled');

    $("#crpXbtn").val("CANCEL");
    $("#crpbtn").val("CROP");
//    $("#usr_orgid").val('Vision');
//    $("#region").val('IN');
//    $("#regionTxt").val('INDIA');
//    $("#mob_a").val('+91');
    $("#usr_lcle").val('en_US');
    $("#usr_lcleText").val('English US');
}




function formsubmit() {
    // //alert("user_reg");
    var userdataVal = userdataValidation();
    //  //alert(userdataVal);
    var doy = $(".dateselector-year option:selected").val();  //dateofbirth
    var dom = $(".dateselector-month option:selected").val();  //dateofbirth
    var dod = $(".dateselector-day option:selected").val();
    if (userdataVal)
    {
        var str = $("#email_id").val();

        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            url: 'emailIdValidate',
            data: {
                email_id: $("#email_id").val()
            },
            cache: false,
            success: function (result) {
                if (result == 'false')
                {
                    userdataVal = false;
                } else {

                    var country = $("#region").val();
                    var plant = $("#usr_plnt").val();
                    var role = $("#usr_rle").val();
                    var locale = $("#usr_lcle").val();
                    var orgnName = $("#usr_orgid").val();

                    console.log("country:::" + country + "::::::");
                    console.log("role:::" + role + "::::::");
                    console.log("locale:::" + locale + "::::::");
                    console.log("plant:::" + plant + "::::::");
                    console.log("orgnName:::" + orgnName + "::::::");


                    $.ajax({
                        type: "post",
                        traditional: true,
                        dataType: 'html',
                        url: 'register',
                        data: {
                            first_name: $("#first_name").val(),
                            middle_name: $("#middle_name").val(),
                            last_name: $("#last_name").val(),
                            nick_name: $("#nick_name").val(),
                            phone_number: $("#tel_no_a").val() + "-" + $("#tel_no_c").val() + "-" + $("#phone_number").val(),
                            mobile_number: $("#mob_a").val() + "-" + $("#mobile_number").val(),
                            email_id: $("#email_id").val(),
                            alternate_email: $("#email").val(),
                            date: dod,
                            month: dom,
                            year: doy,
                            user_name: $("#user_name").val(),
                            password: $("#pwd").val(),
                            confirm_password: $("#confirm_password").val(),
                            country: $("#region").val(),
                            locale: $("#usr_lcle").val(),
                            role: $("#role").val(),
                            add_role: $("#add_rle").val(),
                            report_to: $("#usr_rpt").val(),
                            plant: $("#usr_plnt").val(),
                            address1: $("#address1").val(),
                            address2: $("#address2").val(),
                            experience_summary: $("#experience_summary").val(),
                            filepath: $("#profilepichidden").val(),
                            purposeofReg: $("#purposeofReg").val(),
                            usr_orgid: $("#usr_orgid").val(),
                            instance: $("#usr_Instance").val()
                        },
                        cache: false,
                        success: function (result) {

////alert(result);
                            if (result != null) {
                                var response = JSON.parse(result);

                                var dialogSplitMessage = dialogSplitIconText(response['Message'], response['MessageFlag']);
                                $("#dialog").html(dialogSplitMessage);
                                $("#dialog").dialog({
                                    modal: true,
                                    title: "Message",
                                    height: 'auto',
                                    minHeight: 'auto',
                                    minWidth: 350,
                                    maxWidth: 'auto',
                                    fluid: true,
                                    buttons: {
                                        Ok: function () {
                                            $(this).html("");
                                            $(this).dialog("close");
                                            $(this).dialog("destroy");
                                            if (response['MessageFlag']) {
                                                window.location.href = 'login';
//                                    window.location.reload();

                                            }
                                        }
                                    },
                                    open: function () {
                                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                                        $(this).closest(".ui-dialog").addClass("visionCommonDialog");
                                        $(".visionHeaderMain").css("z-index", "999");
                                        $(".visionFooterMain").css("z-index", "999");
                                    },
                                    beforeClose: function (event, ui)
                                    {
                                        $(".visionHeaderMain").css("z-index", "99999");
                                        $(".visionFooterMain").css("z-index", "99999");
                                    }
                                });
                            }


                        },
                        error: function (e) {
                            //  //alert(e.message)

                            var meg = e.statusText;
                            var status = e.status;
                            if (meg.lastIndexOf("Session Timeout") > -1) {
                                $("#logoutDailog").html("<div style='margin-left: 5%;'><div style='float:left'><img height='30px' src='images/help.png'></img></div><div style='float:left;margin-left: 10%;'>Session Timeout.</div></div>");
                                $("#logoutDailog").dialog({
                                    title: 'Message',
                                    modal: true,
                                    width: 270,
                                    height: 135,
                                    fluid: true,
                                    buttons: {
                                        Ok: function () {
                                            $(this).html("");
                                            $(this).dialog("close");
                                            $(this).dialog("destroy");

                                            window.location.href = "timeout";

                                        }
                                    },
                                    open: function () {
                                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                                        $(".visionHeaderMain").css("z-index", "999");
                                        $(".visionFooterMain").css("z-index", "999");
                                    },
                                    beforeClose: function (event, ui)
                                    {
                                        $(".visionHeaderMain").css("z-index", "99999");
                                        $(".visionFooterMain").css("z-index", "99999");
                                    }
                                });


                            }
                        }

                    });
                }


            },
            error: function (e) {
                //  //alert(e.message)
                sessionTimeout(e);
            }

        });



    }



//    else
//    {
//       $("#divDOM").remove();
//        
//        $("<div/>", {'id': 'divDOM'}).html(UsrErrRslt + "In Personal Data").dialog({
//            'title': 'Error ',
//            modal: true,
//            height: 120,
//            minWidth: 200,
//            maxWidth: 'auto',
//            buttons: {
//                Ok: function () {
//                    $(this).html("");
//                    $(this).dialog("close");
//                    $(this).dialog("destroy");
//                }
//            }});
//    }
    // $("#registerForm").submit();

}
var UsrErrRslt = "";
function userdataValidation()
{
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





    if (charPos == " ")
    {
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
        var id = "#dis_first_name";
        var msg = "Enter First Name";

        err_msg(id, msg);
//        UsrErrRslt += "Enter Name,";
        ActvAccr("basic");

        return false;

    }
    $("#dis_first_name").hide();

    if (!lname) {
        var id = "#dis_last_name";
        var msg = "Enter Last Name";

        err_msg(id, msg);
        UsrErrRslt += "Enter Last Name,";
        ActvAccr("basic");
        return false;
    }
    $("#dis_last_name").hide();
    if (fname && (nameAlpha == false)) {
        var id = "#dis_first_name";
        var msg = "Should be Alphabet";

        err_msg(id, msg);
        ActvAccr("basic");
        return false;
    }
    $("#dis_first_name").hide();
    if (lname && (name1Alpha == false)) {
        var id = "#dis_last_name";
        var msg = "Should be Alphabet";

        err_msg(id, msg);
        ActvAccr("basic");
        return false;
    }
    $("#dis_last_name").hide();
    if (!uname) {
        var id = "#dis_user_name";
        var msg = "Enter UserName";

        err_msg(id, msg);
        //      UsrErrRslt += "Enter USER NAME,";
        ActvAccr("basic");
        return false;
    }
    $("#dis_user_name").hide();

    if (!uReg) {
        var id = "#dis_region";
        var msg = "Select Country";

        err_msg(id, msg);
        //  UsrErrRslt += "Select Region,";
        ActvAccr("basic");
        return false;
    }

    $("#dis_region").hide();
///////////////////////////////////////////////////

    if (!uRle) {
        var id = "#dis_usr_rle";
        var msg = "Select Role";

        err_msg(id, msg);
        //  UsrErrRslt += "Select Role,";
        ActvAccr("basic");
        return false;
    }
    $("#dis_usr_rle").hide();
///////////////////////////////////////////////////
    if (uRle != "FUNCT_CONSULTANT") {
        if (!upass) {
            var id = "#dis_pwd";
            var msg = "Enter Password ";

            err_msg(id, msg);
            //    UsrErrRslt += "Enter Password,";
            ActvAccr("basic");
            return false;
        }
        $("#dis_pwd").hide();
        if (upass && (upasswrd == false)) {
            var id = "#dis_pwd";
            var msg = "Should be Valid Password";

            err_msg(id, msg);
            ActvAccr("basic");
            return false;
        }

        $("#dis_pwd").hide();
        if (!cpass) {
            var id = "#dis_password2";
            var msg = "Enter Confirm Password";

            err_msg(id, msg);
            //     UsrErrRslt += "Enter Confirm Password,";
            ActvAccr("basic");
            return false;
        }
        $("#dis_password2").html("");
        if (upass != cpass) {
            var id = "#dis_password2";
            var msg = "Password do not Match";

            err_msg(id, msg);
            ActvAccr("basic");
            return false;
        }

        $("#dis_password2").hide();

    } else
    {
        $("#password_star").hide();
        $("#pwd").html("disable", "disabled");
        $("#password2_star").hide();
        $("#password2").html("disable", "disabled");
    }

    /////////////////  

    if (!uLcle) {
        var id = "#dis_usr_lcle";
        var msg = "Select Locale";

        err_msg(id, msg);
        //   UsrErrRslt += "Select Locale,";
        ActvAccr("basic");
        return false;
    }
    $("#dis_usr_lcle").hide();
    if (!uOrgId) {
        var id = "#dis_usr_orgid";
        var msg = "Select Organisation ID";

        err_msg(id, msg);
        //   UsrErrRslt += "Select Locale,";
        ActvAccr("basic");
        return false;
    }
    $("#dis_usr_orgid").hide();

    /*updated by ramu Start */
    if ((uRle.indexOf('VM_', 0) != 0) && (uRle.indexOf('CM_', 0) != 0)) {
        if (!uPlnt) {
            var id = "#dis_usr_plnt";
            var msg = "Select Plant";
            err_msg(id, msg);
            ActvAccr("basic");
            return false;
        }
        $("#dis_usr_plnt").hide();
    } else {
        $("#dis_usr_plnt").html("");
    }
    /*updated by ramu end */

    if (uRle.lastIndexOf("REQUESTOR") > -1)
    {
        if (!uRprt) {
            var id = "#dis_usr_rpt";
            var msg = "Select Report To";

            err_msg(id, msg);
            //   UsrErrRslt += "Select Report To,";
            ActvAccr("basic");
            return false;
        }
        $("#dis_usr_rpt").hide();
    } else {
        $("#dis_usr_rpt").html("");
//   return true;
    }

    if ((doy == "") || (dom == "") || (dod == "")) {
        var id = "#dis_dateofbirth";
        var msg = "Select Date Of Birth";

        err_msg(id, msg);
        //   UsrErrRslt += "Select Date Of Birth,";
        ActvAccr("basic");
        return false;
    }
    $("#dis_dateofbirth").hide();


    if (!ucptch)
    {
        $("#dis_Usercaptch").show();
        $("#dis_Usercaptch").html("Should Not Be Null");


        ActvAccr("basic");
        return false;
    }

    if (ucptch && (ucptch == ccptch)) {
        $("#dis_Usercaptch").html("");

    } else
    {
        $("#dis_Usercaptch").html("Captch do not match.");
        $("#dis_Usercaptch").show();

        ActvAccr("basic");
        return false;

    }

    if (!usremail) {
        var id = "#dis_email_id";
        var msg = "Enter Email ID";
        err_msg(id, msg);
        //  UsrErrRslt += "Enter Email Id,";
        ActvAccr("basic");
        return false;
    }
    $("#dis_email_id").hide();
    if (uRle != "FUNCT_CONSULTANT")
    {
        if (usremail && (emailUsr == false)) {
            var id = "#dis_email_id";
            var msg = "Should be Valid Email ID";
            err_msg(id, msg);
            ActvAccr("basic");
            return false;
        } else {
            $("#dis_email").html("");
        }


    } else
    {

        if (usremail && (pilogUsr == false))
        {
            var id = "#dis_email_id";
            var msg = "Should be PiLog Email ID";
            err_msg(id, msg);
            ActvAccr("basic");
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
            success: function (result) {

                ////alert(result);
                // //alert(result);

                if (result == 'false')
                {

                    var id = "#dis_email_id";
                    var msg = "Email Already Exist.";
                    err_msg(id, msg);
                    return false;

                }
            },
            error: function (e) {
                //  //alert(e.message)
                sessionTimeout(e);
            }

        });
    }
    /////////////////////////////////////////////

//    if (!mnumber) {
//        var id = "#dis_mobile_number";
//        var msg = "Enter Mobile Number";
//        err_msg(id, msg);
//        // UsrErrRslt += "Enter Mobile Number,";
//        ActvAccr("basic");
//        return false;
//    }
//    $("#dis_mobile_number").hide();
    if (mnumber && (mobUsr == false)) {
        var id = "#dis_mobile_number";
        var msg = "Mobile No Doesn't Match the Prescribed Format";
        err_msg(id, msg);
        ActvAccr("basic");
        return false;
    }

    if (mobzero.test(mnumber) == true) {
        var id = "#dis_mobile_number";
        var msg = "Enter Valid Mobile Number";
        err_msg(id, msg);
        return false;
    }

    $("#dis_mobile_number").hide();

//    if (uname) {
//
//        if (!userNameValidate()) {
//
//            var id = "#dis_user_name";
//            var msg = "UserName Already Exist.";
//            err_msg(id, msg);
//             ActvAccr("basic");
//            return false;
//        }
//    }

/////////////////


    if ((uTelNo_b && !uTelNo_c) || (!uTelNo_b && uTelNo_c)) {
        var id = ".dis_tel_no_c";

        if (!uTelNo_b) {
            msg = "Enter Telephone number";
        } else {

            msg = "Enter STD Code";
        }
        err_msg(id, msg);
        ActvAccr("Addat");
        return false;
    }

    if (uTelNo_b && uTelNo_c) {
        //   //alert(uTelNo_c+"--"+uTelNo_b);
        /////////////////std code 
        var id = ".dis_tel_no_c";

        var StdZero = /^[0]{1,}$/;
        ;
        var StdRg3 = /^[0-9]{3,5}$/;
        var res = StdRg3.test(uTelNo_c);
        if (StdZero.test(uTelNo_c) == true) {
            var msg = "Enter Valid STD Code";
            err_msg(id, msg);
            return false;
        } else {
            $(id).hide();
        }
        if (res == false)
        {
            var msg = "STD Code Doesn't Match<br> the Prescribed Format";
            err_msg(id, msg);
            ActvAccr("Addat");
            return false;
        } else {
            $(id).hide();
        }
        //////////////////tel no

        var TelReg = /^[0-9]{6,8}$/;
        var res = TelReg.test(uTelNo_b);
        var TelZero = /^[0]{1,}$/;
        if (TelZero.test(uTelNo_b) == true) {
            var msg = "Enter Valid Telephone Number";
            err_msg(id, msg);
            return false;
        } else {
            $(id).hide();
        }

        if (res == false)
        {
            var msg = "Telephone Number Doesn't Match<br> the Prescribed Format";
            err_msg(id, msg);
            ActvAccr("Addat");
            return false;
        } else {
            $(id).hide();

        }

    }
//////////////////
    if ($('#agree').is(':checked') == false) {
        id = "#dis_agree";
        msg = "Please check terms & conditions";
        err_msg(id, msg);

        return false;
    }

    return true;

}
function userprofileValidation()
{
//    var mnumber = $("#mobile_number").val();  //mobilenumber
    var usremail = $("#email_id").val();   //email
    var hidden_usremail = $("#hidden_email_id").val();   //email
    var uRle = $("#usr_rle").val();           //role
    var uRprt = $("#usr_rpt").val();         //report to
    var uPlnt = $("#usr_plnt").val();        //plant  
    //  var uTelNo_a = $("#tel_no_a").val();        //Country code --additional data  
//    var uTelNo_c = $("#tel_no_c").val();        //STD--additional data
//    var uTelNo_b = $("#phone_number").val();        //phone_number--additional data
    var purposeofReg = $("#purposeofReg").val();        //phone_number--additional data


    var alphaNumeric = /^[a-zA-Z0-9&()\s]+$/;
    var alpha = /^[a-zA-Z\s]+$/;


    var mobileNo = /^[0-9]{10}$/;
//    var mobUsr = mobileNo.test(mnumber);
    var mobzero = /^[0]{1,}$/;


    var mail = /^[a-zA-Z0-9_.-]+@[a-zA-Z0-9.]([a-zA-Z0-9.]{1,})+\.[a-zA-Z0-9.]{2,}$/;
    var PiLogmail = /^[a-zA-Z0-9_.-]+@(pilog)+\.[a-zA-Z0-9.]{2,}$/;
    var emailUsr = mail.test(usremail);
    var pilogUsr = PiLogmail.test(usremail);


    var passwrd = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\s).{5,15}$/;


    if (!usremail) {
        var id = "#dis_email_id";
        var msg = "Enter Email ID";
        err_msg(id, msg);
        //  UsrErrRslt += "Enter Email Id,";
        ActvAccr("basic");
        return false;
    }
    $("#dis_email_id").hide();
    if (uRle != "FUNCT_CONSULTANT")
    {
        if (usremail && (emailUsr == false)) {
            var id = "#dis_email_id";
            var msg = "Should be Valid Email ID";
            err_msg(id, msg);
            ActvAccr("basic");
            return false;
        } else {
            $("#dis_email").html("");
        }
    } else
    {

        if (usremail && (pilogUsr == false))
        {
            var id = "#dis_email_id";
            var msg = "Should be PiLog Email ID";
            err_msg(id, msg);
            ActvAccr("basic");
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
            success: function (result) {

                ////alert(result);
                // //alert(result);

                if (result == 'false')
                {

                    var id = "#dis_email_id";
                    var msg = "Email Already Exist.";
                    err_msg(id, msg);
                    return false;

                }
            },
            error: function (e) {
                //  //alert(e.message)

                sessionTimeout(e);
            }

        });
    }

//    if (!mnumber) {
//        var id = "#dis_mobile_number";
//        var msg = "Enter Mobile Number";
//        err_msg(id, msg);
//        // UsrErrRslt += "Enter Mobile Number,";
//        ActvAccr("basic");
//        return false;
//    }
//    $("#dis_mobile_number").hide();
//    if (mnumber && (mobUsr == false)) {
//        var id = "#dis_mobile_number";
//        var msg = "Mobile No Doesn't Match the Prescribed Format";
//        err_msg(id, msg);
//        ActvAccr("basic");
//        return false;
//    }
//
//    if (mobzero.test(mnumber) == true) {
//        var id = "#dis_mobile_number";
//        var msg = "Enter Valid Mobile Number";
//        err_msg(id, msg);
//        return false;
//    }
//
//    $("#dis_mobile_number").hide();

//    if (uname) {
//
//        if (!userNameValidate()) {
//
//            var id = "#dis_user_name";
//            var msg = "UserName Already Exist.";
//            err_msg(id, msg);
//             ActvAccr("basic");
//            return false;
//        }
//    }

/////////////////


//    if ((uTelNo_b && !uTelNo_c) || (!uTelNo_b && uTelNo_c)) {
//        var id = ".dis_tel_no_c";
//
//        if (!uTelNo_b) {
//            msg = "Enter Telephone number";
//        } else {
//
//            msg = "Enter STD Code";
//        }
//        err_msg(id, msg);
//        ActvAccr("Addat");
//        return false;
//    }
//
//    if (uTelNo_b && uTelNo_c) {
//        //   //alert(uTelNo_c+"--"+uTelNo_b);
//        /////////////////std code 
//        var id = ".dis_tel_no_c";
//
//        var StdZero = /^[0]{1,}$/;
//        ;
//        var StdRg3 = /^[0-9]{3,5}$/;
//        var res = StdRg3.test(uTelNo_c);
//        if (StdZero.test(uTelNo_c) == true) {
//            var msg = "Enter Valid STD Code";
//            err_msg(id, msg);
//            return false;
//        }
//        else {
//            $(id).hide();
//        }
//        if (res == false)
//        {
//            var msg = "STD Code Doesn't Match<br> the Prescribed Format";
//            err_msg(id, msg);
//            ActvAccr("Addat");
//            return false;
//        }
//        else {
//            $(id).hide();
//        }
//        //////////////////tel no
//
//        var TelReg = /^[0-9]{6,8}$/;
//        var res = TelReg.test(uTelNo_b);
//        var TelZero = /^[0]{1,}$/;
//        if (TelZero.test(uTelNo_b) == true) {
//            var msg = "Enter Valid Telephone Number";
//            err_msg(id, msg);
//            return false;
//        }
//        else {
//            $(id).hide();
//        }
//
//        if (res == false)
//        {
//            var msg = "Telephone Number Doesn't Match<br> the Prescribed Format";
//            err_msg(id, msg);
//            ActvAccr("Addat");
//            return false;
//        }
//        else {
//            $(id).hide();
//
//        }
//
//    }


    return true;

}


function ActvAccr(param)
{
    if (param == "basic") {
        $("#accordion").accordion({
            collapsible: true,
            active: 0,
        });
    }
    if (param == "Addat") {
        $("#accordion").accordion({
            collapsible: true,
            active: 1,
        });
    }
}
$("#dateofbirth").datepicker({
    changeMonth: true,
    changeYear: true,
    dateFormat: "dd-mm-yy",
    showOn: "button",
    buttonImage: 'pagepics/images/date_picker_icon.png',
    buttonImageOnly: true

});

function RegisterName(ele) {
    var ele = ele;
    var unwantedSpaceValid = unwantedSpaces(ele);
    if (unwantedSpaceValid == true) {
        var str = $("#" + ele).val();
        var err = 'dis_' + ele;
        var patt_an = /^[0-9]+$/;
        var patt_spchar = /^[&()\s]+$/;
        var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;
        var patt = /^[a-zA-Z\s]+$/;

        var res = patt.test(str);

        var res_an = patt_an.test(str);
        var res_spchar = patt_spchar.test(str);
        var res_seq_spchar = patt_seq_spchar.test(str);

        var id = "#" + err;
        if (ele == "first_name") {
            var msg = "Enter First Name";
        }
        if (ele == "last_name") {
            var msg = "Enter Last Name";
        }

        if (res_an == true) {
            var msg = "Enter only Alphabets";
            err_msg(id, msg);
            return false;
        }
        if (res_spchar == true) {
            err_msg(id, msg);
            return false;
        }
        if (res_seq_spchar == true) {
            err_msg(id, msg);
            return false;
        }
        if (str && res == false)
        {
            var msg = "Enter only Alphabets";
            err_msg(id, msg);
            return false;
        }
        $(id).hide();

        str = str.trim();
        $("#" + ele).val(str);

        return true;
    } else
        return false;
}


function IsSpecialCharSpace(e, id) {
    // console.log("AKLSJD");
    var err = 'error_' + id;
    var keyCode = e.keyCode == 0 ? e.charCode : e.keyCode;
    if ((keyCode >= 33 && keyCode <= 37) || (keyCode == 39) || (keyCode >= 42 && keyCode <= 47) || (keyCode >= 58 && keyCode <= 64) || (keyCode >= 91 && keyCode <= 96) || (keyCode >= 123 && keyCode <= 126)) {
        $("#" + err).fadeIn(500);
        $("#" + err).fadeOut(1500);
        return false;
    }
}
function IsSpecialUserName(e, id) {
    // console.log("AKLSJD");
    var err = 'error_' + id;
    var keyCode = e.keyCode == 0 ? e.charCode : e.keyCode;
    if (keyCode == 32) {
        return false;
    }
}
function IsSpecialChar(e, ele) {
    var err = 'error_' + ele;
    var sp_readonly = $('#' + ele).attr("readonly");
    // console.log("Readonly in IsSpecialChar::" + sp_readonly);
    if (sp_readonly != 'readonly')
    {
        var keyCode = e.keyCode == 0 ? e.charCode : e.keyCode;
        if ((keyCode == 32) || (keyCode >= 48 && keyCode <= 57) || (keyCode >= 65 && keyCode <= 90) || (keyCode >= 97 && keyCode <= 122) || (specialKeys.indexOf(e.keyCode) != -1 && e.charCode != e.keyCode)) {
        } else {
            $("#" + err).fadeIn(500);
            $("#" + err).fadeOut(1500);
            return false;
        }
    }
}

function IsSpecialCharPM(e, ele) {
    var err = 'error_' + ele;
    var sp_readonly = $('#' + ele).attr("readonly");
    if (sp_readonly != 'readonly') {
        var keyCode = e.keyCode == 0 ? e.charCode : e.keyCode;
        if ((keyCode >= 48 && keyCode <= 57) || (specialKeys.indexOf(e.keyCode) != -1 && e.charCode != e.keyCode)) {
        } else {
            $("#" + err).fadeIn(500);
            $("#" + err).fadeOut(1500);
            return false;
        }
    }
}

function unwantedSpaces(ele) {
    var str1 = $("#" + ele).val();
    var patt1 = /(^\s*)|(\s*$)/gi;
    var patt2 = /[ ]{2,}/gi;
    if ((patt1.test(str1) == true) || (patt2.test(str1) == true)) {
        str1 = str1.replace(/(^\s*)/gi, "");
        str1 = str1.replace(/[ ]{2,}/gi, " ");
//        if(ele == "user_name"){
//            str1 = str1.replace(/[ ]{1,}/gi, "");
//        }else{
//            str1 = str1.replace(/[ ]{2,}/gi, " ");
//        }

        str1 = str1.trim();
        $("#" + ele).val(str1);
        return true;
    } else
        return true;
}

function IsSpecialChar1(e, ele) {
    var sp_readonly = $('#' + ele).attr("readonly");
    var err = 'error_' + ele;
    if (sp_readonly != 'readonly') {
        var keyCode = e.keyCode == 0 ? e.charCode : e.keyCode;
        if ((keyCode == 47) || (keyCode >= 48 && keyCode <= 57) || (keyCode >= 65 && keyCode <= 90) || (keyCode >= 97 && keyCode <= 122) || (specialKeys.indexOf(e.keyCode) != -1 && e.charCode != e.keyCode)) {
        } else {
            $("#" + err).fadeIn(500);
            $("#" + err).fadeOut(1500);
            return false;
        }
    }
    return ret;
}


function IsBackspace(e, ele) {
    var err = 'error_' + ele;
    var sp_readonly = $('#' + ele).attr("readonly");
    if (sp_readonly != 'readonly') {
        var keyCode = e.keyCode == 0 ? e.charCode : e.keyCode;
        if ((specialKeys.indexOf(e.keyCode) != -1 && e.charCode != e.keyCode)) {
        } else {
            $("#" + err).fadeIn(500);
            $("#" + err).fadeOut(1500);
            return false;
        }
    }
}

function Gen_IsMobNumber(ele) {
    var err = 'dis_' + ele;
    var id = "#" + err;
    var str = $("#" + ele).val();
    var cntry = $("#mob_a").val();
    $(id).html("");


//    if (!str) {
//        var msg = "Enter Mobile Number";
//        err_msg(id, msg);
//        return false;
//    }
    var patt = /^[0-9]{10}$/;
    var res = patt.test(str);

    var patt1 = /(^\s*)|(\s*$)/gi;
    var patt2 = /[ ]{1,}/gi;
    var zero = /^[0]{1,}$/;
    if (zero.test(str) == true) {
        var msg = "Enter Valid Mobile Number";
        err_msg(id, msg);
        return false;
    }
    if ((patt1.test(str) == true) || (patt2.test(str) == true)) {
        str = str.replace(/(^\s*)/gi, "");
        str = str.replace(/[ ]{1,}/gi, "");

        $(ele).val(str);
    }

    if (str) {
        if (res == false)
        {
            var msg = "Mobile Number Doesn't Match<br> the Prescribed Format";
            err_msg(id, msg);
            return false;
        }

        $(id).html("");

    }
    if (str && !cntry)
    {
        var msg = "Please Select Country Code";
        err_msg(id, msg);
        return false;
    } else {
        $(id).hide();
    }

}

function gemail(ele) {
    var err = 'dis_' + ele;
    var id = "#" + err;
    var str = document.getElementById("email_id").value;
    var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;

    var res_seq_spchar = patt_seq_spchar.test(str);
    if (res_seq_spchar == true) {
        $("#dis_email").html("Sequential special </br>characteres are not allowed");
        $("#dis_email").show();
        return false;
    } else {
        $("#dis_email").html("");
    }

    var patt = /^[a-zA-Z0-9_.-]+@[a-zA-Z]([a-zA-Z0-9.]{2,})+\.[a-zA-Z0-9.]{2,}$/;


    if (usrRle != "FUNCT_CONSULTANT") {
        var res = patt.test(str);

        if (res == false)
        {
            var msg = "Enter Valid Email";
            err_msg(id, msg);

        } else {
            document.getElementById("dis_email").innerHTML = "";

        }
    }

}

function Gen_IsTelNumber(ele) {
    var err = 'dis_' + ele;
    var id = "." + err;
    var str = $("#" + ele).val();
    var cntry = $("#tel_no_a").val();


    if (ele == "phone_number") {
        var std = $("#tel_no_c").val();
    }

    var zero = /^[0]{1,}$/;
    if (zero.test(str) == true) {
        var msg = "Enter Valid Telephone Number";
        err_msg(id, msg);
        return false;
    }
    var patt = /^[0-9]{6,8}$/;
    var res = patt.test(str);

    var patt1 = /(^\s*)|(\s*$)/gi;
    var patt2 = /[ ]{1,}/gi;

    if ((patt1.test(str) == true) || (patt2.test(str) == true)) {
        str = str.replace(/(^\s*)/gi, "");
        str = str.replace(/[ ]{1,}/gi, "");

        document.getElementById(ele).value = str;
    }
    if (str && std) {
        if (res == false)
        {
            var msg = "Telephone Number Doesn't Match<br> the Prescribed Format";
            err_msg(id, msg);
            ActvAccr("Addat");
            return false;
        } else {
//            $(".all_errors").hide();
            $(id).hide();
            // return true;
        }
    } else if (str && !std) {
        if (res == false)
        {
            var msg = "Telephone Number Doesn't Match<br> the Prescribed Format";
            err_msg(id, msg);
            ActvAccr("Addat");
            return false;
        } else {
//            var msg = "Enter STD Code";
//            err_msg(id, msg);
//            return false;
        }

    } else if (!str && std) {
        var msg = "Enter Telephone Number";
        err_msg(id, msg);
        ActvAccr("Addat");
        return false;
    } else {
//        $(".all_errors").hide();
        $(id).hide();
        //return true;
    }
    $(id).html("");

    if (str && std && !cntry)
    {
        var msg = "Please Select Country Code";
        err_msg(id, msg);
        return false;
    } else {
        $(id).hide();
    }
}

function Gen_IsStdcode(ele) {

    var err = 'dis_' + ele;
    var id = "." + err;
    var str = document.getElementById(ele).value;
    var cntry = $("#tel_no_a").val();
    if (ele == "tel_no_c") {
        var tel = $("#phone_number").val();
    }

    if (ele == "fax_no_c") {

        var tel = $("#fax_no_b").val();
        console.log(tel);
    }
    var zero = /^[0]{1,}$/;
    if (zero.test(str) == true) {
        var msg = "Enter Valid STD Code";
        err_msg(id, msg);
        return false;
    }

    var patt1 = /(^\s*)|(\s*$)/gi;
    var patt2 = /[ ]{1,}/gi;

    if ((patt1.test(str) == true) || (patt2.test(str) == true)) {
        str = str.replace(/(^\s*)/gi, "");
        str = str.replace(/[ ]{1,}/gi, "");

        document.getElementById(ele).value = str;
    }

    var patt = /^[0-9]{3,5}$/;
    var res = patt.test(str);

    if (str && tel) {
        if (res == false)
        {
            var msg = "STD Code Doesn't Match<br> the Prescribed Format";
            err_msg(id, msg);
            ActvAccr("Addat");
            return false;
        } else {
//            $(".all_errors").hide();
            $(id).hide();
            // return true;
        }
    } else if (str && !tel) {
        if (res == false)
        {
            var msg = "STD Code Doesn't Match<br> the Prescribed Format";
            err_msg(id, msg);
            ActvAccr("Addat");
            return false;
        } else {
            if (ele == "fax_no_c") {
                var msg = "Enter Fax Number";
                err_msg(id, msg);
                return false;
            } else {
                //    var msg = "Enter Telephone Number";
            }

        }
    } else if (!str && tel) {
        var msg = "Enter STD Code ";
        err_msg(id, msg);
        ActvAccr("Addat");
        return false;
    } else {
//        $(".all_errors").hide();
        $(id).hide();
        // return true;
    }

    if (str && tel && !cntry)
    {
        var msg = "Please Select Country Code";
        err_msg(id, msg);
        return false;
    } else {
        $(id).hide();
    }
}

function IsAlphabet(ele) {
    var str = document.getElementById(ele).value;
    if (!str) {
        document.getElementById(err).innerHTML = "";
    } else {
        var err = 'dis_' + ele;
        var patt = /[a-zA-Z\s]+$/;
        var res = patt.test(str);

        if (res == false)
        {
            document.getElementById(err).innerHTML = "Enter only alphabets";
        } else {
            document.getElementById(err).innerHTML = "";
        }
    }
}


//function userNameValidate() {
//   
//    $.ajax({
//        type: "post",
//        traditional: true,
//        dataType: 'html',
//        url: 'userNameValidate',
//        data: {
//            user_name: $("#user_name").val(),
//        },
//        cache: false,
//        success: function(result) {
//
//            // //alert(result);
//
//            if (result == 'false') {
//  
//                var id = "#dis_user_name";
//                var msg = "UserName Already Exist.";
//                err_msg(id, msg);
//             
//
//            } 
//        },
//        error: function(result) {
//            //alert(result);
//        }
//    });
//
//    //return response;
//}

function userNameValidate(ele) {
    var ele = ele;
    var id = "#dis_user_name";
    var unwantedSpaceValid = unwantedSpaces(ele);
    if (unwantedSpaceValid == true) {
        var str = $("#" + ele).val();
        var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;
        var res_seq_spchar = patt_seq_spchar.test(str);
        var frstchar = str.substring(0, 1);
        var patt_first_spcharnum = /^[a-zA-Z]$/;
        var test1 = patt_first_spcharnum.test(frstchar);
//        if (str && (test1 == false))
//        {
//           $("#dis_user_name").html("First letter cannot be number</br> or special char");
//            $("#dis_user_name").show();
//            return false;
//        } else {
//            $("#dis_user_name").html("");
//        }

        if (res_seq_spchar == true) {
            $("#dis_user_name").html("Sequential special </br>characteres are not allowed");
            $("#dis_user_name").show();
            return false;
        } else {
            $("#dis_user_name").html("");
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'html',
                url: 'userNameValidate',
                data: {
                    user_name: $("#user_name").val(),
                },
                cache: false,
                success: function (result) {

                    // //alert(result);

                    if (result == 'false') {

                        id = "#dis_user_name";
                        var msg = "UserName Already Exist.";
                        err_msg(id, msg);


                    }
                },
                error: function (e) {
                    //  //alert(e.message)
                    sessionTimeout(e);
                }

            });

            //return response;
        }
        $(id).hide();
    }
}

$(document).ready(function () {
    //  resetAllValues();


////////////////////////////
    $(".dateselector-month").val("");
    $(".dateselector-day").val("");
    $(".dateselector-month").prop("disabled", "disabled");
    $(".dateselector-day").prop("disabled", "disabled");

    var date = new Date();
    var yy = date.getYear();
    var currentMonth = date.getMonth();
    currentMonth = currentMonth + 1;
//    //alert("date.getMonth()::"+mm); // jan-dec => returns 0-11
    var currentDate = date.getDate();
//    //alert("date.getDate()::"+currentDate); // 16

    var yr = (yy < 1000) ? yy + 1900 : yy;
    var currentYear = yr;
    var list_year = "<option value=''>Year</option>";
    for (yr = yr; yr >= 1900; yr--) {
        list_year += "<option value=" + yr + ">" + yr + "</option>";
    }
    $(".dateselector-year").html(list_year);
    $(".dateselector-year").on('change', function () {

        if (this.value != null || this.value != "") {
            $(".dateselector-month").val("");
            $(".dateselector-day").val("");

            $(".dateselector-month").prop("disabled", false);
            $(".dateselector-day").prop("disabled", "disabled");

            if ($(".dateselector-year").val() == currentYear) {
                for (var mm = 1; mm <= 12; mm++) {
                    if (mm <= currentMonth) {
                        $(".dateselector-month-" + mm).show();
                    } else {
                        $(".dateselector-month-" + mm).hide();
                    }
                }
            } else {
                for (var mm = 1; mm <= 12; mm++) {
                    $(".dateselector-month-" + mm).show();
                }
            }

        }


    });
    $(".dateselector-month").on('change', function () {

        if (this.value != null || this.value != "") {

            $(".dateselector-day").val("");
            $(".dateselector-day").prop("disabled", false);
        }
        var option = $('option:selected', this).attr('class');


        var select_year = $(".dateselector-year").val();
        var select_month = $(".dateselector-month").val();
        var max_day;
        if ((select_year == currentYear) && (select_month == currentMonth)) {
            max_day = currentDate;
        } else if ((select_year % 4 == 0) && (select_month == 02)) {
            max_day = 29;
        } else if ((select_year % 4 != 0) && select_month == 02) {
            max_day = 28;
        } else if ($("option:selected", this).hasClass('thirty1')) {
            max_day = 31;
        } else if ($("option:selected", this).hasClass('thirty0')) {
            max_day = 30;
        }

        var day = max_day;
//        //alert(day);
        var list_day = "<option value=''>Day</option>";
        for (var dy = 1; dy <= day; dy++) {
            if (dy < 10) {
                list_day += "<option value=0" + dy + ">" + dy + "</option>";
            } else {
                list_day += "<option value=" + dy + ">" + dy + "</option>";
            }
        }
        $(".dateselector-day").html(list_day);
    });


    ////////////////////////////////

    $(".ui-dialog-titlebar-close").hover(function () {

        $(this).addClass('ui-button ui-widget ui-state-default ui-corner-all ui-button-icon-only');
    });

    $('#rstCptch').click(function () {
        $("#dis_Usercaptch").html("");
    });

//    $("#user_reg").click(function() {
//        var userdataVal = userdataValidation();
//    });
    $("#crpbtn").click(function ()
    {
        $("#overlay").hide();
        $(".modal1").hide();
        $(".preview-lg").show();

    });
    $("#crpXbtn").click(function ()
    {
        $("#overlay").hide();
        $(".modal1").hide();
    });

    $("#accordion").accordion({
        collapsible: true,
        heightStyle: "content",
        active: 0,
        width: 300
    });



    $('#profilepic').ajaxfileupload({
        'action': 'UploadFile',
        'onComplete': function (response) {
            console.log("On Complete::");
            // $('#upload').hide();
            $('#message').show();

            var statusVal = JSON.stringify(response.message);
            $(".overlay").show();
            $(".modal1").show();

            ////alert(statusVal);
            var obj = JSON.parse(statusVal);
            var filename = obj.filename;
            var filestring = obj.fileString;

            $("#profilepichidden").val(filename);
            $(".cropper-bg").css('background-image', 'url(data:image/png;base64,"+ filestring + ")');
            $(".cropper-bg").css('');
            //
            //
            //$("#img-container").html(" <img id='image' style='vertical-align:middle;'  class='imageclass' src='data:image/png;base64," + filestring + " alt='Picture'/>");
            //    $("#imgDisplay").html(" <img id='image' style='vertical-align:middle;'  class='imageclass' src='data:image/png;base64," + filestring + " alt='Picture'/>");
//			if(statusVal == "false")
//			{
//				$("#message").html("<font color='red'>"+ JSON.stringify(response.message) +" </font>");
//			}	
//			if(statusVal == "true")
//			{
//				$("#message").html("<font color='green'>"+ JSON.stringify(response.message) +" </font>");
//			}			
            $("#oldpic").hide();
        },
        'onStart': function () {
            $('#upload').show();
            $('#message').hide();
            $("#oldpic").show();
            console.log("On start::");
        }
    });


});

function loadnewcaptcha() {
    $.ajax({
        type: "POST",
        url: "captcha",
        traditional: true,
        dataType: 'html',
        cache: false,
        success: function (response) {
            $("#CaptchTxt").html(response);
            $("#Matchcaptch").val(response);
            $('#Usercaptch').val('');
            var $para = $('#CaptchTxt');
            var tokens = response.split('');
            var i = 0;
            i = (i + 1) % tokens.length;
            $para.html('<p style="margin-left: 5px;padding-left: 10px;">' + '<span class=onecolor>' + tokens.slice(0, 1).join('') + '</span>' + '<span class=othercolor>' + tokens.slice(1, 2).join('') + '</span>' +
                    '<span class=onecolor>' + tokens.slice(2, 3).join('') + '</span>' + '<span class=othercolor>' + tokens.slice(3, 4).join('') + '</span>' + '</p>' +
                    '<p style="margin-top: -15px; margin-left: 80px;">' + '<span class=onecolor>' + tokens.slice(4, 5).join('') + '</span>' +
                    '<span class=othercolor>' + tokens.slice(5, 6).join('') + '</span>' + '<span class=onecolor>' + tokens.slice(6, 7).join('') + '</span>'
                    + '<span class=othercolor>' + tokens.slice(7, tokens.length).join('') + '</span>' + '</p>');

        },

        error: function (e) {
            sessionTimeout(e.status);
            alert('Error: ' + e);

        }
    });
}

//function loadnewcaptcha() {
//
//    var req = {};
//    req.url = "captcha";
//    req.async = false;
//    req.type = "POST";
//    req.traditional = true;
//    req.cache = false;
//    req.success = function (response) {
////                   //alert("Captch::"+response);
//        // var Cstring = JSON.parse(response);
//        $("#CaptchTxt").html("" + response);
//        $("#Matchcaptch").val(response);
//        $('#Usercaptch').val('');
//        var $para = $('#CaptchTxt');
//        var tokens = $para.text().split('');
//        var i = 0;
//        i = (i + 1) % tokens.length;
//        $para.html('<p style="margin-left: 5px;padding-left: 10px;">' + '<span class=onecolor>' + tokens.slice(0, 1).join('') + '</span>' + '<span class=othercolor>' + tokens.slice(1, 2).join('') + '</span>' +
//                '<span class=onecolor>' + tokens.slice(2, 3).join('') + '</span>' + '<span class=othercolor>' + tokens.slice(3, 4).join('') + '</span>' + '</p>' +
//                '<p style="margin-top: -15px; margin-left: 80px;">' + '<span class=onecolor>' + tokens.slice(4, 5).join('') + '</span>' +
//                '<span class=othercolor>' + tokens.slice(5, 6).join('') + '</span>' + '<span class=onecolor>' + tokens.slice(6, 7).join('') + '</span>'
//                + '<span class=othercolor>' + tokens.slice(7, tokens.length).join('') + '</span>' + '</p>');
//    };
////        $para.html('<p>'+'<span class=onecolor>' + tokens.slice(0, 1).join('') + '</span>'+'<span class=onecolor>' + tokens.slice(1, 2).join('') + '</span>'+
////                '<span class=onecolor>' + tokens.slice(2, 3).join('') + '</span>'+'<span class=onecolor>' + tokens.slice(3, 4).join('') + '</span>'+
////                '<span class=onecolor>' + tokens.slice(4, 5).join('') + '</span>'+'<span class=onecolor>' + tokens.slice(5, 6).join('') + '</span>'+'</p>'+
////                '<p style="margin-top: -18px; margin-left: 100px;">'+'<span class=othercolor>' + tokens.slice(6, 7).join('') + '</span>'+'<span class=othercolor>' + tokens.slice(7, 8).join('') + '</span>'+
////                '<span class=othercolor>' + tokens.slice(8, 9).join('') + '</span>'+'<span class=othercolor>' + tokens.slice(9, 10).join('') + '</span>'+
////                '<span class=othercolor>' + tokens.slice(10, 11).join('') + '</span>'+' <span class=othercolor>' + tokens.slice(11, tokens.length).join('') + '</span>'+'</p>');
////    };
//
//    $.ajax(req);
//
//}
//;

///////////////////////////////////
var myInter = setInterval(function () {
    loadnewcaptcha()
}, 54000);
function stpcptchIntr() {
    var usrcaptcval = $("#Usercaptch").val();
    clearInterval(myInter);
}
/////////////////////////////////////

//function checkCaptchMatch() {
//
//    var captch = $("#Matchcaptch").val();
//    var confirmcaptch = $("#Usercaptch").val();
//
////    if (!confirmcaptch)
////    {
////        $("#dis_Usercaptch").html("Should Not Be Null");
////    } 
//    if (captch == confirmcaptch) {
//        $("#dis_Usercaptch").html("");
//        return true;
//    }
//    else
//    {
//        $("#dis_Usercaptch").html("Captch do not match.");
//         $("#dis_Usercaptch").show();
//return false;
//    }
//
//}


function checkCaptchMatch() {

    var captch = $("#Matchcaptch").val();
    var TrimCaptch = $("#Usercaptch").val();
//    alert("captch::::" + captch + "::TrimCaptch:::" + TrimCaptch);
    if (TrimCaptch != '') {
        var charPos = TrimCaptch.charAt(4);

        if (charPos == " ")
        {
            var res = TrimCaptch.replace(charPos, "");
            var confirmcaptch = res;
////alert(confirmcaptch);
        } else {
            var confirmcaptch = $("#Usercaptch").val();
        }

//    if (!confirmcaptch)
//    {
//        $("#dis_Usercaptch").html("Should Not Be Null");
//    } 
        if (confirmcaptch && (captch != confirmcaptch)) {
//            alert(captch);
            $("#dis_Usercaptch").html("Captch do not match.");
            $("#dis_Usercaptch").show();
            return false;

        } else
        {
            $("#dis_Usercaptch").html("");
            return true;
        }


        // $("#dis_Usercaptch").html("");
    } else {
        $("#dis_Usercaptch").html("Captcha Can't be empty.");
        $("#dis_Usercaptch").show();
        return false;
    }

}

function uemail(ele) {
    var err = 'dis_' + ele;
    var id = "#" + err;
    var str = $("#email_id").val();
    var hidden_str = $("#hidden_email_id").val();
    if (!str) {
        var msg = "Enter  Email Address";
        err_msg(id, msg);
        return false;
    }
    var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;

    var res_seq_spchar = patt_seq_spchar.test(str);
    if (res_seq_spchar == true) {
        $("#dis_email_id").html("Sequential special </br>characteres are not allowed");
        $("#dis_email_id").show();
        return false;
    } else {
        $("#dis_email_id").html("");
    }
    var patt = /^[a-zA-Z0-9_.-]+@[a-zA-Z]([a-zA-Z0-9.]{2,})+\.[a-zA-Z0-9.]{2,}$/;

    var res = patt.test(str);

    if (str && res == false)
    {
        var msg = "Enter Valid Email";

        err_msg(id, msg);
        return true;
    } else if (str != hidden_str) {
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
            success: function (result) {

                ////alert(result);
                //  //alert(result);

                if (result == 'false')
                {

                    var id = "#dis_email_id";
                    var msg = "Email Already Exist.";
                    err_msg(id, msg);
                    return false;

                }
            },
            error: function (e) {
                //  //alert(e.message)

                sessionTimeout(e);
            }

        });
    }

    // $("#dis_email_id").hide();
}//uemail

/////////////////////////// FORGOT PASSWORD###################

function resetvalues() {


    window.location.href = 'login';

}
function validemail(ele) {
    var err = 'dis_' + ele;
    var id = "#" + err;
    var str = $("#email_id").val();
//    if (!str) {
//        var msg = "Enter  Email Address";
//        err_msg(id, msg);
//        return false;
//    }
    var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;

    var res_seq_spchar = patt_seq_spchar.test(str);
    if (res_seq_spchar == true) {
        $("#dis_email_id").html("Sequential special </br>characteres are not allowed");
        $("#dis_email_id").show();
        return false;
    } else {
        $("#dis_email_id").html("");
    }
    var patt = /^[a-zA-Z0-9_.-]+@[a-zA-Z]([a-zA-Z0-9.]{2,})+\.[a-zA-Z0-9.]{2,}$/;

    var res = patt.test(str);

    if (str && res == false)
    {
        var msg = "Enter Valid Email";

        err_msg(id, msg);
        return true;
    } else {
        ////alert("hiii");
        $("#dis_email_id").hide();
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            url: 'emailIdValidateforgot',
            data: {
                email_id: $("#email_id").val()
            },
            cache: false,
            success: function (result) {

                // //alert(result);
                // //alert(result);



                if (result == 'true')
                {


                    return true;

                } else {
                    var id = "#dis_email_id";
                    var msg = result;
//                    var msg = "Email ID Not Exist in Database.";
                    err_msg(id, msg);
                }
            },
            error: function (e) {
                //  //alert(e.message)
                sessionTimeout(e);
            }

        });
    }

    // $("#dis_email_id").hide();
}//uemail



//function CheckPasswordForgt(ele) {
//    var re;
//    var err = 'dis_' + ele;
//    var id = "#" + err;
//    var str = $("#old_password").val();
//     var newpwd = $("#pwd").val();
//    var confirmpassword = $("#password2").val();
//    
//
//    var msg = "Password must contain minimum </br> of five characters with one number (0-9),<br>one lowercase letter (a-z),<br>one Uppercase letter (A-Z) & <br>one Special Character!";
//    if (!str)
//    {
//        var msg = "Should not be null";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//    
//    if (str && str.length < 5)
//    {
//
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//    re = /[0-9]/;
//    if (str && !re.test(str))
//    {
////        var msg = "Password must contain atleast one number (0-9)!";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//    re = /[a-z]/;
//    if (str && !re.test(str))
//    {
////        var msg = "password must contain at least one lowercase le<br>ter (a-z)!";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//    re = /[A-Z]/;
//    if (str && !re.test(str))
//    {
////        var msg = "password must contain at least one Uppercase letter (A-Z)!";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//    re = /([!,%,&,@,#,$,^,*,?,_,~])/;
//    if (str && !re.test(str))
//    {
////        var msg = "password must contain at least one Special Character!";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//
//    var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;
//    var res_seq_spchar = patt_seq_spchar.test(str);
//
//    if (res_seq_spchar == true) {
//        $("#dis_old_password").html("Sequential special </br>characteres are not allowed");
//        $("#dis_old_password").show();
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//
//    var patt = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\s).{5,15}$/;
//    var res = patt.test(str);
//    if (str && res == false)
//    {
//        var msg = "Enter Password As Specified";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//
//    $("#dis_old_password").hide();
//     if (newpwd)
//    {
//       CheckPasswordNew("pwd");
//    }
//    
//    if (confirmpassword)
//    {
//       checkPasswordMatchReType();
//    }
//   
//    
////$("#restpassword").prop('disabled', false);
//}
//;
//
//
//function CheckPasswordNew(ele) {
//    var re;
//    var err = 'dis_' + ele;
//    var id = "#" + err;
//    var str = $("#pwd").val();
//
//    var msg = "Password must contain minimum </br> of five characters with one number (0-9),<br>one lowercase letter (a-z),<br>one Uppercase letter (A-Z) & <br>one Special Character!";
//    if (str && str.length < 5)
//    {
//
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//        
//    }
//    re = /[0-9]/;
//    if (str && !re.test(str))
//    {
////        var msg = "Password must contain atleast one number (0-9)!";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//    re = /[a-z]/;
//    if (str && !re.test(str))
//    {
////        var msg = "password must contain at least one lowercase le<br>ter (a-z)!";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//    re = /[A-Z]/;
//    if (str && !re.test(str))
//    {
////        var msg = "password must contain at least one Uppercase letter (A-Z)!";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//    re = /([!,%,&,@,#,$,^,*,?,_,~])/;
//    if (str && !re.test(str))
//    {
////        var msg = "password must contain at least one Special Character!";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//
//    var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;
//    var res_seq_spchar = patt_seq_spchar.test(str);
//
//    if (res_seq_spchar == true) {
//        $("#dis_pwd").html("Sequential special </br>characteres are not allowed");
//        $("#dis_pwd").show();
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//
//    var patt = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[^a-zA-Z0-9])(?!.*\s).{5,15}$/;
//    var res = patt.test(str);
//    if (str && res == false)
//    {
//        var msg = "Enter Password As Specified";
//        err_msg(id, msg);
//        $("#restpassword").prop('disabled', true);
//        return false;
//    }
//
//
//    var old_password = $("#old_password").val();
//    if (old_password == str) {
//        console.log("str::" + str);
//        console.log("old_password::" + old_password);
//        
//       
//        $("#dis_pwd").show();
//        $("#dis_pwd").html("Passwords should not be match");
//        $("#restpassword").prop('disabled', true);
//
//        return false;
//    }
//    else
//    {
//        $("#dis_pwd").html("");
//        $("#password2").prop('disabled', false);
//        $("#restpassword").prop('disabled', true);
//
//        //return true;
////        $("#pwd").show();
////        $("#pwd").html("Passwords do not match.");
//
//    }
//
//
//
//}
;

////////////////////////FORGOT PASSWORD/////
function forgotpassword() {


    var email_id = $("#email_id").val();
    var user_id = $("#user_id").val();
    // //alert("email_id"+email_id);
    if (!email_id) {
        var id = "#dis_email_id";
        var msg = "Enter Email ID";
        err_msg(id, msg);
        return false;
    } else if (!user_id) {
        var id = "#dis_user_id";
        var msg = "Enter User ID";
        err_msg(id, msg);
        return false;
    } else
    {
        $('#wait').show();
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            url: 'forgotpassword',
            data: {
                email_id: $("#email_id").val(),
                user_id: $("#user_id").val()

            },
            cache: false,
            success: function (result) {
                // //alert(result);
                $('#wait').hide();
                var dialogSplitMessage = dialogSplitIconText(result, "Y");
                $("#dialog").html(dialogSplitMessage);
                $("#dialog").dialog({
                    modal: true,
                    title: "Message",
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: 350,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: {
                        Ok: function () {

                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");

                            if (result.lastIndexOf("password") > -1) {
                                window.location.href = 'login';
                                // window.location.reload();

                            }
                            $('#email_id').val('');
                            $('#user_id').val('');
                        }
                    },
                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(this).closest(".ui-dialog").addClass("visionCommonDialog");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                    },
                    beforeClose: function (event, ui)
                    {
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }
                });


            },
            error: function (e) {
                //  //alert(e.message)
                sessionTimeout(e);
            }

        });
    }

}
function resetpassword() {
    var pwd = $("#pwd").val();
    var password2 = $("#password2").val();
    if (!pwd) {
        var id = "#dis_pwd";
        var msg = "Enter Password";
        err_msg(id, msg);
        return false;
    } else if (!password2) {
        var id = "#dis_password2";
        var msg = "Re-Type New Password";
        err_msg(id, msg);
        return false;
    } else if (!genericpasswordvalidation("pwd")) {
        return false;

    }
//   else if(!genericpasswordvalidation("password2")){
//       return false;
//       
//   }

    else if (pwd == password2) {
        $("#dis_password2").html("");
        var secretKey = $('meta[name=keygeneration]').attr("content");
        var password = $("#pwd").val();
        var encryptedPassword = CryptoJS.AES.encrypt(password, secretKey);
        $("#pwd").val(encryptedPassword);

        var confirm_password = $("#password2").val();
        var encryptedConfirmPassword = CryptoJS.AES.encrypt(confirm_password, secretKey);
        $("#password2").val(encryptedConfirmPassword);
//        $("#restpassword").prop('disabled', false);

        var temp = $("#pers_Id").val();
        // alert(temp);
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            url: 'resetpassword',
            data: {
                password: $("#pwd").val(),
                confirm_password: $("#password2").val(),
                persId: $("#pers_Id").val()
            },
            cache: false,
            success: function (result) {
                var dialogSplitMessage = dialogSplitIconText(result, "Y");
                $("#dialog").html(dialogSplitMessage);
                $("#dialog").dialog({
                    modal: true,
                    title: "Message",
                    height: 'auto',
                    minHeight: 'auto',
                    minWidth: 350,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: {
                        Ok: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");

                            if (result.lastIndexOf("Sucessfully") > -1) {
                                window.location.href = 'login';
                                // window.location.reload();

                            }
                        }
                    },
                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(this).closest(".ui-dialog").addClass("visionCommonDialog");
                        $(".visionHeaderMain").css("z-index", "999");
                        $(".visionFooterMain").css("z-index", "999");
                    },
                    beforeClose: function (event, ui)
                    {
                        $(".visionHeaderMain").css("z-index", "99999");
                        $(".visionFooterMain").css("z-index", "99999");
                    }
                });


            },
            error: function (e) {
                //  alert(e.message)
                sessionTimeout(e);
            }

        });


    } else
    {
        $("#dis_password2").show();
        $("#dis_password2").html("Passwords do not match.");
//        $("#restpassword").prop('disabled', true);



    }

    // alert("resetpassword");

}



function checkPasswordMatchReType() {


    var pass = $("#pwd").val();
    var confirmPassword = $("#password2").val();
    if (pass == confirmPassword) {
        $("#dis_password2").html("");

//        $("#restpassword").prop('disabled', false);
        // console.log("pass::" + pass);
        //console.log("confirmPassword::" + confirmPassword);

    } else
    {
        $("#dis_password2").show();
        $("#dis_password2").html("Passwords do not match.");
//        $("#restpassword").prop('disabled', true);
    }

}
function changepassword() {

    var old_password = $("#old_password").val();
    var pwd = $("#pwd").val();
    var password2 = $("#password2").val();
//     //alert("email_id"+email_id);
    if (!old_password) {
        var id = "#dis_old_password";
        var msg = "Enter Old Password";
        err_msg(id, msg);
        return false;
    } else if (!pwd) {
        var id = "#dis_pwd";
        var msg = "Enter New Password";
        err_msg(id, msg);
        return false;
    } else if (!password2) {
        var id = "#dis_password2";
        var msg = "Re-Type New Password";
        err_msg(id, msg);
        return false;
    } else if (old_password == pwd) {
        var id = "#dis_password2";
        var msg = "Old password and new password are same";
        err_msg(id, msg);
        return false;
    } else if (!genericpasswordvalidation("old_password") || !genericpasswordvalidation("pwd")) {
        return false;

    } else if (pwd == password2)
    {
        var secretKey = $('meta[name=keygeneration]').attr("content");
        var old_password = $("#old_password").val();
        var encryptedOldPassword = CryptoJS.AES.encrypt(old_password, secretKey);
        $("#old_password").val(encryptedOldPassword);

        var password = $("#pwd").val();
        var encryptedPassword = CryptoJS.AES.encrypt(password, secretKey);
        $("#pwd").val(encryptedPassword);

        var confirm_password = $("#password2").val();
        var encryptedConfirmPassword = CryptoJS.AES.encrypt(confirm_password, secretKey);
        $("#password2").val(encryptedConfirmPassword);
        $('#wait').show();
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'json',
            url: 'changepassword',
            data: {
                old_password: $("#old_password").val(),
                password: $("#pwd").val(),
                confirm_password: $("#password2").val()

            },
            cache: false,
            success: function (response) {
                $('#wait').hide();
////alert(result);
                if (response != null) {
                    var dialogSplitMessage = dialogSplitIconText(response['message'], "Y");
                    $("#dialog").html(dialogSplitMessage);
                    $("#dialog").dialog({
                        modal: true,
                        title: "Message",
                        height: 'auto',
                        minHeight: 'auto',
                        minWidth: 350,
                        maxWidth: 'auto',
                        fluid: true,
                        buttons: {
                            Ok: function () {
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                //mainModuleChooser
                                if (response['messageFlag']) {
                                    var passLogoutFlag = $("#ssPasswordLogoutFlag").val();
                                    if (passLogoutFlag != null && passLogoutFlag != '' && passLogoutFlag == 'Y') {
                                        window.location.href = 'appLogout';
                                    } else {
                                        window.location.href = 'mainModuleChooser';
                                    }

//                                    window.location.href = 'mainModuleChooser';
                                }

                            }
                        },
                        open: function () {
                            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                            $(this).closest(".ui-dialog").addClass("visionCommonDialog");
                            $(".visionHeaderMain").css("z-index", "999");
                            $(".visionFooterMain").css("z-index", "999");
                        },
                        beforeClose: function (event, ui)
                        {
                            $(".visionHeaderMain").css("z-index", "99999");
                            $(".visionFooterMain").css("z-index", "99999");
                        }
                    });
                }



            },
            error: function (e) {
                //  //alert(e.message)
                sessionTimeout(e);
            }

        });

    }

}
function sendEmail() {
    //function sendEmail(id) {
    var ele = 'email_id';
    var ele_user = 'user_id';
    var email_id = $("#email_id").val();
    var user_id = $("#user_id").val();
    if (!email_id) {
        var id = "#dis_email_id";
        var msg = "Enter Email ID";
        err_msg(id, msg);
        return false;
    } else if (!user_id) {
        var id = "#dis_user_id";
        var msg = "Enter User ID";
        err_msg(id, msg);
        return false;
    } else
    {

        var err = 'dis_' + ele;
        var id = "#" + err;
        var str = $("#email_id").val();

        var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;
        var res_seq_spchar = patt_seq_spchar.test(str);
        if (res_seq_spchar == true) {
            $("#dis_email_id").html("Sequential special </br>characteres are not allowed");
            $("#dis_email_id").show();
            return false;
        } else {
            $("#dis_email_id").html("");
        }
        var patt = /^[a-zA-Z0-9_.-]+@[a-zA-Z]([a-zA-Z0-9.]{2,})+\.[a-zA-Z0-9.]{2,}$/;

        var res = patt.test(str);

        if (str && res == false)
        {
            var msg = "Enter Valid Email ID";
            err_msg(id, msg);
            return false;

        } else {

            $("#dis_email_id").hide();
            $.ajax({type: "post",
                traditional: true,
                dataType: 'html',
                url: 'emailIdValidateforgot', data: {
                    email_id: $("#email_id").val()
                },
                cache: false, success: function (result) {




                    if (result == 'true')
                    {

                        validate();
                        //                                     return true;
                    } else {
                        var id = "#dis_email_id";
                        var msg = result;
                        //                    var msg = "Email ID Not Exist in Database.";
                        // $("#email").attr('disabled','disabled'); // disable
                        err_msg(id, msg);

                    }
                },
                error: function (e) {
                    //  //alert(e.message)
                    sessionTimeout(e);
                }

            });
        }



    }

}
function hideMessage() {
    $(".all_errors").hide();
}

function validate() {
    $('#wait').show();
    var ele = 'email_id';
    var ele_user = 'user_id';
    var email_id = $("#email_id").val();
    var user_id = $("#user_id").val();
    if (!user_id) {
        var id = "#dis_user_id";
        var msg = "Enter User ID";
        err_msg(id, msg);
        $('#wait').hide();
        return false;
    } else
    if (!email_id) {
        var id = "#dis_email_id";
        var msg = "Enter Email ID";
        err_msg(id, msg);
        $('#wait').hide();
        return false;
    } else
    {
        var err = 'dis_' + ele;
        var id = "#" + err;
        var str = $("#email_id").val();

        var patt_seq_spchar = /^(.*)[`!@#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-][~!@`#$%^&*()=_+{}\[\]\\|:;"'<>?,./\-](.*)$/;
        var res_seq_spchar = patt_seq_spchar.test(str);
        if (res_seq_spchar == true) {
            $("#dis_email_id").html("Sequential special </br>characteres are not allowed");
            $("#dis_email_id").show();
            $('#wait').hide();
            return false;
        } else {
            $("#dis_email_id").html("");
        }
        var patt = /^[a-zA-Z0-9_.-]+@[a-zA-Z]([a-zA-Z0-9.]{2,})+\.[a-zA-Z0-9.]{2,}$/;

        var res = patt.test(str);

        if (str && res == false)
        {
            var msg = "Enter Valid Email ID";
            err_msg(id, msg);
            $('#wait').hide();
            return false;

        } else {
            $("#dis_email_id").hide();
            $("#dis_user_id").hide();
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'html',
                url: 'forgotpassword',
                data: {email_id: $("#email_id").val(),
                    user_id: $("#user_id").val()

                },
                cache: false,
                success: function (result) {
                    // //alert(result);
                    $('#wait').hide();
                    var dialogSplitMessage = dialogSplitIconText(result, "Y");
                    $("#dialog").html(dialogSplitMessage);
                    $("#dialog").dialog({
                        modal: true,
                        title: "Message",
                        width: 500,
                        minHeight: 'auto',
                        maxHeight: 'auto',
                        fluid: true,
                        buttons: {
                            Ok: function () {

                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");

                                if (result.lastIndexOf("password") > -1) {
                                    window.location.href = 'login';
                                    // window.location.reload();

                                }
                                $('#email_id').val('');
                                $('#user_id').val('');
                            }
                        },
                        open: function () {
                            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                            $(this).closest(".ui-dialog").addClass("visionForgotDialog");
                            $(".visionHeaderMain").css("z-index", "999");
                            $(".visionFooterMain").css("z-index", "999");
                        },
                        beforeClose: function (event, ui)
                        {
                            $(".visionHeaderMain").css("z-index", "99999");
                            $(".visionFooterMain").css("z-index", "99999");
                        }
                    });


                },
                error: function (e) {
                    //  //alert(e.message)
                    sessionTimeout(e);
                }

            });
        }
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
            err_msg(errorID, msg);
            return false;
        }
    } else {
        msg = "Password should not blank.";
        err_msg(errorID, msg);
        return false;
    }

}
function fetchOrgId() {

    var usr_orgid = $("#usr_orgid").val();
    console.log("usr_orgid:::" + usr_orgid);

    var parameter = "";
    var passwordRegex = "";
    var passwordDesc = "";

    $.ajax({type: "get",
        traditional: true,
        dataType: 'html',
        url: 'fetchOrgId',
        data: {
            usr_orgid: $("#usr_orgid").val()
        },
        cache: false, success: function (result) {
            var json = JSON.parse(result);
            for (var i = 0; i < json.length; i++) {
                var data = json[i];
                parameter += data.parameter;
                passwordRegex += data.passwordRegex;
                passwordDesc += data.passwordDesc;

                passwordRegex += ":::";
                passwordDesc += ":::";
                $("#pwd").attr('data-pswdDesc', passwordDesc);
                $("#pwd").attr('data-pswdRegex', passwordRegex);
            }

        },
        error: function (result) {
//                //alert(result);
            sessionTimeout(result);
        }
    });
}


function userRegisterDetails() {
    var userCreateVal = userCreationValidation();

    var userId = $('#user_name').val();
    if (userId != null && userId != "")
    {
        userId = userId.toUpperCase();
    }
    var emailId = $('#email_id').val();
    if (emailId != null && emailId != "")
    {
        emailId = emailId.toUpperCase();
    }
    var fname = $('#first_name').val();
    if (fname != null && fname != "")
    {
        fname = fname.toUpperCase();
    }
    var lname = $('#last_name').val();
    if (lname != null && lname != "")
    {
        lname = lname.toUpperCase();
    }
    var mobileno = $('#mobile_number').val();
    var country = $('#region').val();
    // var countryCode = $('#region').val();
    var locale = $('#usr_lcle').val();
//    var role = $('#usr_rleTxt').val();
    var roleId = $('#usr_rleTxt').val();
    var role = $('#usr_rle').val();
    var plant = $('#usr_plntTxt').val();
    var instance = $('#usr_Instance').val();
    var region = $('#usr_region').val();
    var trainingDate = $('#register-form-date-picker').val();
    var trainerName = $('#trainer_name_txt').val();
    if (trainerName != null && trainerName != "")
    {
        trainerName = trainerName.toUpperCase();
    }
    var trainerLoc = $('#usr_TrainingLoc').val();
    if (trainerLoc != null && trainerLoc != "")
    {
        trainerLoc = trainerLoc.toUpperCase();
    }
    var trainerComments = $('#Trainer_comments').val();
    if (trainerComments != null && trainerComments != "")
    {
        trainerComments = trainerComments.toUpperCase();
    }
    var orgnId = $('#orgnId').val();
    var dob = $('#dob').val();
    var gender = $('#gender').val();
    var usr_addrs = $('#usr_addrs').val();
    var app_mail_id = $('#approverMailId').val();
    var mgr_mail_id = $('#managerMailId').val();
    // var userId = $('#user_name').val();
    if (userCreateVal) {
        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            url: 'userRegisterInfo',
            data: {
                'userId': userId,
                'emailId': emailId,
                'fname': fname,
                'lname': lname,
                'mobileno': mobileno,
                'country': country,
                'locale': locale,
                'role': role,
                'roleId': roleId,
                'plant': plant,
                'instance': instance,
                'region': region,
                'trainingDate': trainingDate,
                'trainerName': trainerName,
                'trainerLoc': trainerLoc,
                'trainerComments': trainerComments,
                'orgnId': orgnId,
                'dob': dob,
                'gender': gender,
                'usr_addrs': usr_addrs,
                'approverMailId': app_mail_id,
                'managerMailId': mgr_mail_id





            },
            cache: false,
            success: function (response) {
                console.log("userdetails:::" + response);
                if (response != null) {
                    //var response = JSON.parse(result);
//var message=result.Message;
                    //  var dialogSplitMessage = dialogSplitIconText(result['Message']);
                    var result = JSON.parse(response);
                    var message = result.Message;
                    $("#dialog").html(message);
                    $("#dialog").dialog({
                        modal: true,
                        title: "Message",
                        height: 'auto',
                        minHeight: 'auto',
                        minWidth: 350,
                        maxWidth: 'auto',
                        fluid: true,
                        buttons: {
                            Ok: function () {
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                if (result.MessageFlag != null && result.MessageFlag == 'Y') {
                                    window.location.href = 'login';
//                                    window.location.reload();

                                }

                            }
                        },
                        open: function () {
                            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                            $(this).closest(".ui-dialog").addClass("visionCommonDialog");
                            $(".visionHeaderMain").css("z-index", "999");
                            $(".visionFooterMain").css("z-index", "999");
                        },
                        beforeClose: function (event, ui)
                        {
                            $(".visionHeaderMain").css("z-index", "99999");
                            $(".visionFooterMain").css("z-index", "99999");
                        }
                    });
                }


            },
            error: function (result) {
//                //alert(result);
                sessionTimeout(result);
            }
        });
    }

}


function retrieveUserInfo() {
    var userSelected = $("input[name='fields']:checked").val();
    var userInputVal = $('#getId').val();
    if (userSelected != null && userSelected != "" && userInputVal != null && userInputVal != "") {
        $('#error_radio_btn').css('display', 'none');
        $('#error_input_val').css('display', 'none');

        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'html',
            url: 'retrieveUserInfo',
            data: {
                'userSelected': userSelected,
                'userId': userInputVal
            },
            cache: false,
            success: function (response) {
                var result = JSON.parse(response);
                if (result != null && result != "") {
                    var flag = result.MessageFlag;
                    var message = result.Message;
                    $("#dialog").html(message);
                    $("#dialog").dialog({
                        modal: true,
                        title: "Message",
                        height: 'auto',
                        minHeight: 'auto',
                        minWidth: 350,
                        maxWidth: 'auto',
                        fluid: true,
                        buttons: {
                            Ok: function () {
                                $(this).html("");
                                $(this).dialog("close");
                                $(this).dialog("destroy");
                                if (flag != null && flag == 'Y') {
                                    var username = result['domainLoginID'];
                                    $('#user_name').val(username);
                                    $('#email_id').val(result['email']);
                                    $('#first_name').val(result['firstName']);
                                    $('#last_name').val(result['lastName']);
                                    $('#region').val(result['country']);
                                    $('#regionTxt').val(result['countryDescr']);
                                    $('#mob_a').val(result['countrySTDCode']);
                                    $('#mobile_number').val(result['mobile']);

                                }

                            }
                        },
                        open: function () {
                            $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                            $(this).closest(".ui-dialog").addClass("visionCommonDialog");
                            $(".visionHeaderMain").css("z-index", "999");
                            $(".visionFooterMain").css("z-index", "999");
                        },
                        beforeClose: function (event, ui)
                        {
                            $(".visionHeaderMain").css("z-index", "99999");
                            $(".visionFooterMain").css("z-index", "99999");
                        }
                    });
                }

            },
            error: function (result) {
//                //alert(result);
                sessionTimeout(result);
            }
        });
    } else if (userSelected == null || userSelected == '') {
        $('#error_radio_btn').html('Please select some value');
        $('#error_radio_btn').show();
        $('#error_input_val').hide();
    } else if (userInputVal == null || userInputVal == '') {
        $('#error_input_val').html('Please enter input value');
        $('#error_radio_btn').hide();
        $('#error_input_val').show();

    }


}
function userCreationValidation() {
    //var flag;
    var flag=true;
    $('.visionBasicUserRegisterTable :input').each(function () {
        var id = $(this).attr("id");
        var title = $(this).attr("data-name");
        var inputVal = $("#" + id).val();
        var mandatoryVal = $(this).attr("data-mandatory");
        if (mandatoryVal != null && mandatoryVal == 'M') {
            if (inputVal != null && inputVal != '') {
                $(".all_errors").hide();
              } 
            else {
                var errid = "#dis_" + id;
                var msg = "" + title + " should not be empty";
                err_msg(errid, msg);
                flag = false;
              }

        }
    });
  return flag;
}
