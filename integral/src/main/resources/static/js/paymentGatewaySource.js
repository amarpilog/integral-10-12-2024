function checkTotal($this, tabCompId) {
    var annulaCount = 0;
    var totalCount = 0;
    var subTotal = 0;
    $("#" + tabCompId + " tbody tr").each(function () {
        var tdArray = this.cells;
        if (tdArray != null && tdArray.length != 0) {
            var subScriptionObj = {};
            var apiId = $(tdArray[0]).find("input").val();
            var volume = $(tdArray[2]).find("select").val();
            var usersCount = $(tdArray[3]).find("select").val();
            var priceEnc = $(tdArray[4]).find("input").val();
            var words = CryptoJS.enc.Base64.parse(priceEnc);
            var price = CryptoJS.enc.Utf8.stringify(words);
            var duration = $(tdArray[5]).find("select").val();
            if (volume != null && volume != ''
                    && usersCount != null && usersCount != ''
                    && price != null && price != ''
                    && duration != null && duration != ''
                    ) {
                totalCount++;
                if (duration == '12') {
                    annulaCount++;
                }
                var totalValue = parseInt(volume) * parseInt(usersCount) * parseInt(price) * parseInt(duration);
                if (!isNaN(totalValue)) {
                    subTotal += totalValue;
                    $(tdArray[6]).find("span").html(totalValue.toLocaleString());
                }

            }

        }

    });
    var finalPrice = subTotal;
    var discount = 0;
    $("#" + tabCompId + "_total").html(subTotal.toLocaleString());
    if (totalCount != 0 && totalCount == annulaCount && !isNaN(subTotal)) {
        discount = (parseInt(subTotal) / 100) * 10;
        finalPrice = parseInt(subTotal) - parseInt(discount);
    }
    $("#" + tabCompId + "_discount").html(discount.toLocaleString());
    $("#" + tabCompId + "_final").html(finalPrice.toLocaleString());
}

function paymentProcess(componentId) {
    var ssUsername = $("#ssUsername").val();
    var selectedRole = "";
    if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {
        var subScriptionObjArray = [];
        $("#" + componentId + " tbody tr").each(function () {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var subScriptionObj = {};
                var apiId = $(tdArray[0]).find("input").val();
                var volume = $(tdArray[2]).find("select").val();
                var usersCount = $(tdArray[3]).find("select").val();
                var priceEnc = $(tdArray[4]).find("input").val();
                var words = CryptoJS.enc.Base64.parse(priceEnc);
                var price = CryptoJS.enc.Utf8.stringify(words);
                //  var price = CryptoJS.enc.Base64.stringify(priceEnc);
                var duration = $(tdArray[5]).find("select").val();
                if (volume != null && volume != ''
                        && usersCount != null && usersCount != ''
                        && price != null && price != ''
                        && duration != null && duration != ''
                        ) {
                  var  selectedRoleEnc = $(tdArray[7]).find("input").val();
                   var selectedRoleWords = CryptoJS.enc.Base64.parse(selectedRoleEnc);
                   selectedRole = CryptoJS.enc.Utf8.stringify(selectedRoleWords);
                    subScriptionObj['apiId'] = apiId;
                    subScriptionObj['volume'] = volume;
                    subScriptionObj['usersCount'] = usersCount;
                    subScriptionObj['price'] = price;
                    subScriptionObj['duration'] = duration;
                    subScriptionObjArray.push(subScriptionObj);
                }

            }

        });
        if (subScriptionObjArray != null && subScriptionObjArray.length != 0) {
            var options = {};
            $.ajax({
                datatype: "json",
                type: "POST",
                url: "processCheckout",
                data: {
                    subScriptionObjArray: JSON.stringify(subScriptionObjArray),
                    selectedRole:selectedRole
                },
                traditional: true,
                cache: false,
                success: function (response) {
                    ajaxStop();
                    if (response != null && !jQuery.isEmptyObject(response)) {
                        options = response;
                        options.handler = function (successResponse) {
                            paymentResponse(successResponse, subScriptionObjArray, response, "Y",selectedRole);
                        };
                        options.notes = {
                            "address": "note value"
                        };
                        options.theme = {
                            "color": "#9932CC"
                        };
                        var propay = new Razorpay(options);
                        propay.on('payment.failed', function (failResponse) {
                            paymentResponse(failResponse, subScriptionObjArray, response, "N",selectedRole);
                        });

                        propay.open();
                    }
                },
                error: function (e) {
                    ajaxStop();
                    alert('Error: ' + e);

                }
            });
        }else{
          var modalObj = {
              title:'Message',
              body:'Please select atleast one api to checkout.'
          };
          var buttonArray = [
              {
                  text:'Ok',
                  isCloseButton:true,
                   
              }
          ];
          modalObj['buttons'] = buttonArray;
          createModal("modalDailogDiv",modalObj);
        }
       
    }else{
        $('#loginModel').modal('show'); 
    }   

}


function paymentResponse(payResponse, subScriptionObjArray, paymentOptions, paymentFlag,selectedRole) {
    if (payResponse != null) {
        var paymentResultObj = {};
        if (paymentFlag == 'Y') {
            paymentResultObj['org_name'] = payResponse['org_name'];
            paymentResultObj['razorpay_order_id'] = payResponse['razorpay_order_id'];
            paymentResultObj['razorpay_payment_id'] = payResponse['razorpay_payment_id'];
            paymentResultObj['payment_id'] = payResponse['razorpay_payment_id'];
            paymentResultObj['razorpay_signature'] = payResponse['razorpay_signature'];
        } else {
            paymentResultObj['errorcode'] = payResponse.error.code;
            paymentResultObj['errordescription'] = payResponse.error.description;
            paymentResultObj['errorreason'] = payResponse.error.reason;
            paymentResultObj['errorsource'] = payResponse.error.source;
            paymentResultObj['errorstep'] = payResponse.error.step;
            paymentResultObj['order_id'] = payResponse.error.metadata.order_id;
            paymentResultObj['payment_id'] = payResponse.error.metadata.payment_id;
        }

        $.ajax({
            datatype: "json",
            type: "POST",
            url: "saveSubscriptionDetails",
            data: {
                subScriptionObjArray: JSON.stringify(subScriptionObjArray),
                paymentOptions: JSON.stringify(paymentOptions),
                paymentResultObj: JSON.stringify(paymentResultObj),
                paymentFlag: paymentFlag,
                selectedRole:selectedRole
            },
            traditional: true,
            cache: false,
            success: function (response) {
                ajaxStop();
                if (response != null && !jQuery.isEmptyObject(response)) {
                    var modalObj = {
                        title: 'Message',
                        body: response['message']
                    };
                    var buttonArray = [
                        {
                            text: 'Ok',
                            click:function(){
                                 if (response['flag']) {
                                    fetchTabGrid(5);
                                }
                               
                            },
                            isCloseButton: true
                        }
                    ];
                    modalObj['buttons'] = buttonArray;
                    createModal("modalDailogDiv", modalObj);
                }
            },
            error: function (e) {
                ajaxStop();
                alert('Error: ' + e);

            }
        });
    }

}
function generateAPIKey($this,apiId) {
    $.ajax({
        datatype: "json",
        type: "POST",
        url: "genrateAPIKey",
        data: {
           API_ID:apiId
        },
        traditional: true,
        cache: false,
        success: function (response) {
            ajaxStop();
            if (response != null && !jQuery.isEmptyObject(response)) {
                var modalObj = {
                    title: 'Message',
                    body: response['message']
                };
                var buttonArray = [
                    {
                        text: 'Ok',
                        click: function () {
                            if (response['flag']) {
                                var tr = $this.closest("tr");
                                var tdArray = tr.cells;
                                $(tdArray[2]).html(response['API_KEY']);
                            }
                           
                        },
                        isCloseButton: true
                    }
                ];
                modalObj['buttons'] = buttonArray;
                createModal("modalDailogDiv", modalObj);
            }
        },
        error: function (e) {
            ajaxStop();
            alert('Error: ' + e);

        }
    });
}

function navigateToMDRM($this,apiId,navigationURL) {
    //navigationUrlForm
   
 var  ssUsername =  $("#ssUsername").val();
 var  ssCloudV10URL =  $("#ssCloudV10URL").val();
    var secretKey = $('meta[name=keygeneration]').attr("content");
    var rsPassword ="Pilog@123";
    var encryptedPassword = CryptoJS.AES.encrypt(rsPassword, secretKey);
    $("#navigationUrlForm").empty();
       $("#navigationUrlForm").attr("target", "_blank");
       $("#navigationUrlForm").attr("action", ssCloudV10URL+"loginhandler");
       
    $("#navigationUrlForm").append("<input type='hidden' name='rsUsername' value='"+ssUsername+"'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='rsPasswordHid' value='"+encryptedPassword+"'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='language' value='en_US'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='API_ID' value='"+apiId+"'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='navigationURL' value='"+navigationURL+"'/>");
    var csrfToken = $("input[name='_csrf']").val();
    if (csrfToken != null && csrfToken != '') {
        var csrf = "<input type='hidden' name='_csrf' value='" + csrfToken + "'/>";
        $("#navigationUrlForm").append(csrf);
    }
    $("#navigationUrlForm").submit();
}