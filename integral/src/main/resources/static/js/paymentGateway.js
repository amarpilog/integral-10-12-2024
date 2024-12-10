//function checkTotal($this, tabCompId) {
//    var annulaCount = 0;
//    var totalCount = 0;
//    var subTotal = 0;
//    $("#" + tabCompId + " tbody tr").each(function () {
//        var tdArray = this.cells;
//        if (tdArray != null && tdArray.length != 0) {
//            var subScriptionObj = {};
//            var apiId = $(tdArray[0]).find("input").val();
//            var volume = $(tdArray[2]).find("select").val();
//            var usersCount = $(tdArray[3]).find("select").val();
//            var priceEnc = $(tdArray[4]).find("input").val();
//            var words = CryptoJS.enc.Base64.parse(priceEnc);
//            var price = CryptoJS.enc.Utf8.stringify(words);
//            var duration = $(tdArray[5]).find("select").val();
//            if (volume != null && volume != ''
//                    && usersCount != null && usersCount != ''
//                    && price != null && price != ''
//                    && duration != null && duration != ''
//                    ) {
//                totalCount++;
//                if (duration == '12') {
//                    annulaCount++;
//                }
//                var totalValue = parseInt(volume) * parseInt(usersCount) * parseInt(price) * parseInt(duration);
//                if (!isNaN(totalValue)) {
//                    subTotal += totalValue;
//                    $(tdArray[6]).find("span").html(totalValue.toLocaleString());
//                }
//
//            } else {  //Onkar added else block for showing total as blank 
//                $(tdArray[6]).find("span").html("");
//            }
//
//        }
//
//    });
//    $("#addToCartCount").val(totalCount);
//    $("#addToCartBtnId").html("Add to Cart (" + totalCount + ")");//for increase add to cart count.
//    var finalPrice = subTotal;
//    var discount = 0;
//    $("#" + tabCompId + "_total").html(subTotal.toLocaleString());
//    if (totalCount != 0 && totalCount == annulaCount && !isNaN(subTotal)) {
//        discount = (parseInt(subTotal) / 100) * 10;
//        finalPrice = parseInt(subTotal) - parseInt(discount);
//    }
//    $("#" + tabCompId + "_discount").html(discount.toLocaleString());
//    $("#" + tabCompId + "_final").html(finalPrice.toLocaleString());
//}


function checkTotal($this, tabCompId) {
    var annulaCount = 0;
    var totalCount = 0;
    var subTotal = 0;
    var taxPercentageValue = 0;
    var upgradeSubscriptionId = $("#upgradeSubscriptionId").val();
    if (upgradeSubscriptionId != null && upgradeSubscriptionId != "") {
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
                var duration = ($(tdArray[5]).find("select").val() != null) ? ($(tdArray[5]).find("select").val()) : ($(tdArray[5]).find("input").val());

                var existingPricePerDay = $(tdArray[9]).find("input").val();
                var completedDays = $(tdArray[10]).find("input").val();
                var remainingDays = $(tdArray[11]).find("input").val();
                var totalNoOfDays = parseInt(completedDays) + parseInt(remainingDays);
                var totalPricePaid = parseInt(existingPricePerDay) * (parseInt(completedDays) + parseInt(remainingDays));
                var refundPriceForRemainingDays = parseInt(existingPricePerDay) * (parseInt(remainingDays) - 1);

                if (volume != null && volume != ''
                        && usersCount != null && usersCount != ''
                        && price != null && price != ''
                        && duration != null && duration != ''
                        ) {
                    totalCount++;
                    if (duration == '12') {
                        annulaCount++;
                    }
                    var newpricePerDay = (parseInt(volume) * parseInt(usersCount) * parseInt(price) * parseInt(duration)) / totalNoOfDays;
                    var priceForRemainingDays = newpricePerDay * remainingDays;
                    var totalValue = priceForRemainingDays - refundPriceForRemainingDays;


                    if (!isNaN(totalValue)) {
                        subTotal += totalValue;
                        $(tdArray[6]).find("span").html(totalValue.toLocaleString());
                    }

                } else {  //Onkar added else block for showing total as blank 
                    $(tdArray[6]).find("span").html("");

                }

            }

        });
    } else {
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
                var duration = ($(tdArray[5]).find("select").val() != null) ? ($(tdArray[5]).find("select").val()) : ($(tdArray[5]).find("input").val());


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

                } else {  //Onkar added else block for showing total as blank 
                    $(tdArray[6]).find("span").html("");

                }

            }

        });

    }
    $("#addToCartCount").val(totalCount);
    $("#addToCartBtnId").html("Add to Cart (" + totalCount + ")");//for increase add to cart count.
    var finalPrice = subTotal;
    var discount = 0;

    var totalTaxPercent = $("#" + tabCompId + " tfoot").children('tr:first').children('th:first').attr('totaltax-percent');

    var taxPercentageValue = (subTotal / 100) * parseInt(totalTaxPercent);
    $("#" + tabCompId + "_tax").html(taxPercentageValue.toLocaleString());
    finalPrice = subTotal + taxPercentageValue;

    $("#" + tabCompId + "_total").html(subTotal.toLocaleString());
    if (totalCount != 0 && totalCount == annulaCount && !isNaN(subTotal)) {
        discount = (parseInt(subTotal) / 100) * 10;
        finalPrice = parseInt(subTotal) - parseInt(discount);
    }
    $("#" + tabCompId + "_discount").html(discount.toLocaleString());
    $("#" + tabCompId + "_final").html(finalPrice.toLocaleString());
}

//function paymentProcess(componentId) {
//    var ssUsername = $("#ssUsername").val();
//    var selectedRole = "";
//    if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {
//        var subScriptionObjArray = [];
//        $("#" + componentId + " tbody tr").each(function () {
//            var tdArray = this.cells;
//            if (tdArray != null && tdArray.length != 0) {
//                var subScriptionObj = {};
//                var apiId = $(tdArray[0]).find("input").val();
//                var volume = $(tdArray[2]).find("select").val();
//                var usersCount = $(tdArray[3]).find("select").val();
//                var priceEnc = $(tdArray[4]).find("input").val();
//                var words = CryptoJS.enc.Base64.parse(priceEnc);
//                var price = CryptoJS.enc.Utf8.stringify(words);
//                //  var price = CryptoJS.enc.Base64.stringify(priceEnc);
//                var duration = $(tdArray[5]).find("select").val();
//                if (volume != null && volume != ''
//                        && usersCount != null && usersCount != ''
//                        && price != null && price != ''
//                        && duration != null && duration != ''
//                        ) {
//                    var selectedRoleEnc = $(tdArray[7]).find("input").val();
//                    var selectedRoleWords = CryptoJS.enc.Base64.parse(selectedRoleEnc);
//                    selectedRole = CryptoJS.enc.Utf8.stringify(selectedRoleWords);
//                    subScriptionObj['apiId'] = apiId;
//                    subScriptionObj['volume'] = volume;
//                    subScriptionObj['usersCount'] = usersCount;
//                    subScriptionObj['price'] = price;
//                    subScriptionObj['duration'] = duration;
//                    subScriptionObjArray.push(subScriptionObj);
//                }
//
//            }
//
//        });
//        if (subScriptionObjArray != null && subScriptionObjArray.length != 0) {
//            var options = {};
//            $.ajax({
//                datatype: "json",
//                type: "POST",
//                url: "processCheckout",
//                data: {
//                    subScriptionObjArray: JSON.stringify(subScriptionObjArray),
//                    selectedRole: selectedRole
//                },
//                traditional: true,
//                cache: false,
//                success: function (response) {
//                    ajaxStop();
//                    if (response != null && !jQuery.isEmptyObject(response)) {
//                        options = response;
//                        options.handler = function (successResponse) {
//                            paymentResponse(successResponse, subScriptionObjArray, response, "Y", selectedRole);
//                        };
//                        options.notes = {
//                            "address": "note value"
//                        };
//                        options.theme = {
//                            "color": "#9932CC"
//                        };
//                        var propay = new Razorpay(options);
//                        propay.on('payment.failed', function (failResponse) {
//                            paymentResponse(failResponse, subScriptionObjArray, response, "N", selectedRole);
//                        });
//
//                        propay.open();
//                    }
//                },
//                error: function (e) {
//                    ajaxStop();
//                    alert('Error: ' + e);
//
//                }
//            });
//        } else {
//            var modalObj = {
//                title: 'Message',
//                body: 'Please select atleast one api to checkout.'
//            };
//            var buttonArray = [
//                {
//                    text: 'Ok',
//                    isCloseButton: true,
//
//                }
//            ];
//            modalObj['buttons'] = buttonArray;
//            createModal("modalDailogDiv1", modalObj);
//        }
//
//    } else {
//        $('#loginModel').modal('show');
//    }
//
//}

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
                var duration = ($(tdArray[5]).find("select").val() != null) ? ($(tdArray[5]).find("select").val()) : ($(tdArray[5]).find("input").val());
                var subscriptionId = $(tdArray[8]).find("input").val();
                if (volume != null && volume != ''
                        && usersCount != null && usersCount != ''
                        && price != null && price != ''
                        && duration != null && duration != ''
                        ) {
                    var selectedRoleEnc = $(tdArray[7]).find("input").val();
                    var selectedRoleWords = CryptoJS.enc.Base64.parse(selectedRoleEnc);
                    selectedRole = CryptoJS.enc.Utf8.stringify(selectedRoleWords);
                    subScriptionObj['apiId'] = apiId;
                    subScriptionObj['volume'] = volume;
                    subScriptionObj['usersCount'] = usersCount;
                    subScriptionObj['price'] = price;
                    subScriptionObj['duration'] = duration;
                    subScriptionObj['subscriptionId'] = subscriptionId;
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
                    selectedRole: selectedRole
                },
                traditional: true,
                cache: false,
                success: function (response) {
                    ajaxStop();
                    if (response != null && !jQuery.isEmptyObject(response)) {
                        options = response;
                        options.handler = function (successResponse) {
                            paymentResponse(successResponse, subScriptionObjArray, response, "Y", selectedRole);
                        };
                        options.notes = {
                            "address": "note value"
                        };
                        options.theme = {
                            "color": "#9932CC"
                        };
                        var propay = new Razorpay(options);
                        propay.on('payment.failed', function (failResponse) {
                            paymentResponse(failResponse, subScriptionObjArray, response, "N", selectedRole);
                        });

                        propay.open();
                    }
                },
                error: function (e) {
                    ajaxStop();
                    alert('Error: ' + e);

                }
            });
        } else {
            var modalObj = {
                title: 'Message',
                body: 'Please select atleast one api to checkout.'
            };
            var buttonArray = [
                {
                    text: 'Ok',
                    isCloseButton: true,

                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("modalDailogDiv1", modalObj);
        }

    } else {
        $('#loginModel').modal('show');
    }

}

//function paymentResponse(payResponse, subScriptionObjArray, paymentOptions, paymentFlag, selectedRole) {
//    if (payResponse != null) {
//        var paymentResultObj = {};
//        if (paymentFlag == 'Y') {
//            paymentResultObj['org_name'] = payResponse['org_name'];
//            paymentResultObj['razorpay_order_id'] = payResponse['razorpay_order_id'];
//            paymentResultObj['razorpay_payment_id'] = payResponse['razorpay_payment_id'];
//            paymentResultObj['payment_id'] = payResponse['razorpay_payment_id'];
//            paymentResultObj['razorpay_signature'] = payResponse['razorpay_signature'];
//        } else {
//            paymentResultObj['errorcode'] = payResponse.error.code;
//            paymentResultObj['errordescription'] = payResponse.error.description;
//            paymentResultObj['errorreason'] = payResponse.error.reason;
//            paymentResultObj['errorsource'] = payResponse.error.source;
//            paymentResultObj['errorstep'] = payResponse.error.step;
//            paymentResultObj['order_id'] = payResponse.error.metadata.order_id;
//            paymentResultObj['payment_id'] = payResponse.error.metadata.payment_id;
//        }
//
//        $.ajax({
//            datatype: "json",
//            type: "POST",
//            url: "saveSubscriptionDetails",
//            data: {
//                subScriptionObjArray: JSON.stringify(subScriptionObjArray),
//                paymentOptions: JSON.stringify(paymentOptions),
//                paymentResultObj: JSON.stringify(paymentResultObj),
//                paymentFlag: paymentFlag,
//                selectedRole: selectedRole
//            },
//            traditional: true,
//            cache: false,
//            success: function (response) {
//                ajaxStop();
//                if (response != null && !jQuery.isEmptyObject(response)) {
//                    var modalObj = {
//                        title: 'Message',
//                        body: response['message']
//                    };
//                    var buttonArray = [
//                        {
//                            text: 'Ok',
//                            click: function () {
//                                if (response['flag']) {
//                                    fetchTabGrid(5);
//                                }
//
//                            },
//                            isCloseButton: true
//                        }
//                    ];
//                    modalObj['buttons'] = buttonArray;
//                    createModal("modalDailogDiv", modalObj);
//                }
//            },
//            error: function (e) {
//                ajaxStop();
//                alert('Error: ' + e);
//
//            }
//        });
//    }
//
//}

function paymentResponse(payResponse, subScriptionObjArray, paymentOptions, paymentFlag, selectedRole) {
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
                selectedRole: selectedRole,
                apiId: subScriptionObjArray[0]['apiId'],
                subscriptionId: subScriptionObjArray[0]['subscriptionId']
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

function generateAPIKey($this, apiId) {
    $.ajax({
        datatype: "json",
        type: "POST",
        url: "genrateAPIKey",
        data: {
            API_ID: apiId
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

function navigateToMDRM($this, apiId, navigationURL, apiEncStr) {
    //navigationUrlForm

    var ssUsername = $("#ssUsername").val();
    var ssCloudV10URL = $("#ssCloudV10URL").val();
// var  ssCloudV10URL =  "http://localhost:8080/VisionDev/";    
    var secretKey = $('meta[name=keygeneration]').attr("content");
    var rsPassword = "Pilog@123";
    var encryptedPassword = CryptoJS.AES.encrypt(rsPassword, secretKey);
    $("#navigationUrlForm").empty();
    $("#navigationUrlForm").attr("target", "_blank");
    $("#navigationUrlForm").attr("action", ssCloudV10URL + "loginhandler");

    $("#navigationUrlForm").append("<input type='hidden' name='rsUsername' value='" + ssUsername + "'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='rsPasswordHid' value='" + encryptedPassword + "'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='language' value='en_US'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='API_ID' value='" + apiId + "'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='navigationURL' value='" + navigationURL + "'/>");
    $("#navigationUrlForm").append("<input type='hidden' name='apiEncStr' value='" + apiEncStr + "'/>");
    var csrfToken = $("input[name='_csrf']").val();
    if (csrfToken != null && csrfToken != '') {
        var csrf = "<input type='hidden' name='_csrf' value='" + csrfToken + "'/>";
        $("#navigationUrlForm").append(csrf);
    }
    $("#navigationUrlForm").submit();
}
function addToCart(componentId) {
    var ssUsername = $("#ssUsername").val();
    var selectedRole = "";
    if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {
        var subScriptionObjArray = [];
        $("#" + componentId + " tbody tr").each(function () {
            var tdArray = this.cells;
            if (tdArray != null && tdArray.length != 0) {
                var subScriptionObj = {};
                var apiId = $(tdArray[0]).find("input").val();
                var functionName = tdArray[1].innerHTML;
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
                    var selectedRoleEnc = $(tdArray[7]).find("input").val();
                    var selectedRoleWords = CryptoJS.enc.Base64.parse(selectedRoleEnc);
                    selectedRole = CryptoJS.enc.Utf8.stringify(selectedRoleWords);
                    subScriptionObj['apiId'] = apiId;
                    subScriptionObj['functionName'] = functionName;
                    subScriptionObj['volume'] = volume;
                    subScriptionObj['usersCount'] = usersCount;
                    subScriptionObj['price'] = price;
                    subScriptionObj['duration'] = duration;
                    subScriptionObjArray.push(subScriptionObj);
                }

            }

        });
        if (subScriptionObjArray != null && subScriptionObjArray.length != 0) {
            $.ajax({
                datatype: "json",
                type: "POST",
                url: "addToCart",
                data: {
                    subScriptionObjArray: JSON.stringify(subScriptionObjArray),
                    selectedRole: selectedRole,
                    ssUsername: ssUsername,
                    highLevelMenu: $("#highLevelMenu").val()
                },
                traditional: true,
                cache: false,
                success: function (response) {
                    ajaxStop();
                    console.log(response);
                    if (response != null && !jQuery.isEmptyObject(response)) {
                        var insertCount = response['insertCount'];
                        var cartCount = parseInt($("#cartCount").text());
                        var modalObj = {
                            title: 'Message',
                            body: ' Api Successfully added to cart'
                        };
                        var buttonArray = [
                            {
                                text: 'Ok',
                                click: function () {
                                    //location.reload();
                                    $("#cartCount").text(insertCount + cartCount);
                                },
                                isCloseButton: true
                            }
                        ];
                        modalObj['buttons'] = buttonArray;
                        createModal("modalDailogDiv1", modalObj);
                    }
                },
                error: function (e) {
                    ajaxStop();
                    alert('Error: ' + e);
                }
            });
        } else {
            var modalObj = {
                title: 'Message',
                body: 'Please select atleast one api to Add to Cart.'
            };
            var buttonArray = [
                {
                    text: 'Ok',
                    isCloseButton: true
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("modalDailogDiv1", modalObj);
        }
    } else {
        $('#loginModel').modal('show');
    }
}
function cartApiPaymentProcess() {
    var ssUsername = $("#ssUsername").val();
    var selectedRole = "";

    if (ssUsername != null && ssUsername != '' && ssUsername != 'null') {
        var rowindexes = $('#myCartGridid').jqxGrid('getselectedrowindexes');
        var subScriptionObjArray = [];
        for (var i = 0; i < rowindexes.length; i++) {
            var duration = 0;
            var rowData = $('#myCartGridid').jqxGrid('getrowdata', rowindexes[i]);
            var subScriptionObj = {};
            var volume = rowData.volume;
            var usersCount = rowData.userCount;
            var words = CryptoJS.enc.Base64.parse(rowData.price);
            var priceStr = rowData.price;
            var price = priceStr.replace("$", "");
            var durationStr = rowData.duration.trim();
            if (durationStr != null && durationStr != '' && durationStr == "Monthly") {
                duration = 1;
            } else if (durationStr != null && durationStr != '' && durationStr == "Quaterly") {
                duration = 3;
            } else if (durationStr != null && durationStr != '' && durationStr == "Half-Yearly") {
                duration = 6;
            } else if (durationStr != null && durationStr != '' && durationStr == "Annually") {
                duration = 12;
            }
            if (volume != null && volume != ''
                    && usersCount != null && usersCount != ''
                    && price != null && price != ''
                    && duration != null && duration != ''
                    ) {
                // subScriptionObj['apiId'] = apiId;
                subScriptionObj['volume'] = volume;
                subScriptionObj['usersCount'] = usersCount;
                subScriptionObj['price'] = price;
                subScriptionObj['duration'] = duration;
                subScriptionObjArray.push(subScriptionObj);
            }
        }

        if (subScriptionObjArray != null && subScriptionObjArray.length != 0) {
            var options = {};
            $.ajax({
                datatype: "json",
                type: "POST",
                url: "processCheckout",
                data: {
                    subScriptionObjArray: JSON.stringify(subScriptionObjArray)
                },
                traditional: true,
                cache: false,
                success: function (response) {
                    ajaxStop();
                    if (response != null && !jQuery.isEmptyObject(response)) {
                        options = response;
                        options.handler = function (successResponse) {
                            paymentResponse(successResponse, subScriptionObjArray, response, "Y", selectedRole);
                        };
                        options.notes = {
                            "address": "note value"
                        };
                        options.theme = {
                            "color": "#9932CC"
                        };
                        var propay = new Razorpay(options);
                        propay.on('payment.failed', function (failResponse) {
                            paymentResponse(failResponse, subScriptionObjArray, response, "N", selectedRole);
                        });

                        propay.open();
                    }
                },
                error: function (e) {
                    ajaxStop();
                    alert('Error: ' + e);
                }
            });
        } else {
            var modalObj = {
                title: 'Message',
                body: 'Please select atleast one api to checkout.'
            };
            var buttonArray = [
                {
                    text: 'Ok',
                    isCloseButton: true,
                }
            ];
            modalObj['buttons'] = buttonArray;
            createModal("modalDailogDiv1", modalObj);
        }
    } else {
        $('#loginModel').modal('show');
    }
}
function upgradeSubscription($this, apiId, subscriptionId) {
    $('#subscriptionId').val(subscriptionId);
    $('#modifySubscriptionFlag').val("Y");
    var length = $('#cloudTabs').jqxTabs('length');
    var subscriptionTabIndex;
    for (var i = 0; i < length; i++) {
        var title = $('#cloudTabs').jqxTabs('getTitleAt', i);
        if (title != null && title.toUpperCase() == "SUBSCRIPTION") {
            subscriptionTabIndex = i;
            break;
        }
    }
    $('#cloudTabs').jqxTabs('select', subscriptionTabIndex);

}

function renewSubscription($this, apiId, subscriptionId) {
    $('#subscriptionId').val(subscriptionId);
    $('#renewSubscriptionFlag').val("Y");
    var length = $('#cloudTabs').jqxTabs('length');
    var subscriptionTabIndex;
    for (var i = 0; i < length; i++) {
        var title = $('#cloudTabs').jqxTabs('getTitleAt', i);
        if (title != null && title.toUpperCase() == "SUBSCRIPTION") {
            subscriptionTabIndex = i;
            break;
        }
    }
    $('#cloudTabs').jqxTabs('select', subscriptionTabIndex);
}


function fetchSubscriptions(divId, subsCatogory, highLevelMenu, selectedGridId) {
    var divHtml = $("#" + divId).children('div:first').html();
    if (divHtml != null && divHtml != "") {
        $("#" + divId).children('div:first').toggle();
        var heading = $("#" + divId).children('span:first').text();
        if (heading.indexOf("▼") > -1) {
            if (subsCatogory == "HISTORY") {
                $("#" + divId).children('span:first').text("▶ History")
            } else if (subsCatogory == "UPCOMING") {
                $("#" + divId).children('span:first').text("▶ Upcoming")
            }
//             heading.replace("&#9660;","&#9654;");
        } else if (heading.indexOf("▶") > -1) {
            if (subsCatogory == "HISTORY") {
                $("#" + divId).children('span:first').text("▼ History")
            } else if (subsCatogory == "UPCOMING") {
                $("#" + divId).children('span:first').text("▼ Upcoming")
            }

//             heading.replace("&#9654;","&#9660;");
        }
    } else {
        $.ajax({
            datatype: "html",
            type: "POST",
            url: "getSubscriptionAPI",
            data: {
                highLevelMenu: highLevelMenu,
                tabComponentId: selectedGridId,
                subsCatogory: subsCatogory
            },
            traditional: true,
            cache: false,
            success: function (response) {
                ajaxStop();
                if (response != null) {
                    var result = response['result'];

                    var div = $("#" + divId).children('div:first');
                    $("#" + divId).children('div:first').html("");
                    $("#" + divId).children('div:first').html(result);

                    if (subsCatogory == "HISTORY") {
                        $("#" + divId).children('span:first').text("▼ History")
                    } else if (subsCatogory == "UPCOMING") {
                        $("#" + divId).children('span:first').text("▼ Upcoming")
                    }
                    $("#" + divId).children('div:first').toggle();
                }

            },
            error: function (e) {
                ajaxStop();
                alert('Error: ' + e);
            }
        });
    }

}

function showApiSubscriptionHistory(apiId, highLevelMenuId, apiDesc) {
    $.ajax({
        datatype: "html",
        type: "POST",
        url: "getSubscriptionAPI",
        data: {
            highLevelMenu: highLevelMenuId,
            tabComponentId: "ABCD",
            subsCatogory: "HISTORY"
        },
        traditional: true,
        cache: false,
        success: function (response) {
            ajaxStop();
            if (response != null) {
                var result = response['result'];
                var modalObj = {
                    title: 'History',
                    body: result
                };

                var modalDivId = "cloudViewHistoryModal";

                $("#" + modalDivId).html("");

                $("#" + modalDivId).addClass("modal fade");
                $("#" + modalDivId).attr("role", "dialog");
                $("#" + modalDivId).attr("data-backdrop", "static");
                $("#" + modalDivId).css("padding-top", "58px");
                var modalContant = ''
                        + '<div class="modal-dialog" style="max-width:1000px !important;">'
                        + '<div class="modal-content">'
                        + '<div class="modal-header">'
                        + '<h4 class="modal-title text-center">' + modalObj['title'] + '</h4>'
                        + '<button type="button" class="close" data-toggle="modal" data-target="#' + modalDivId + '"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>'
                        + '</div>'
                        + '<div class="modal-body">'
                        + modalObj['body']
                        + ' </div>';
                modalContant += '</div>'
                        + '</div>';
                $("#" + modalDivId).html(modalContant);


                $('#' + modalDivId).modal('show');

            }

        },
        error: function (e) {
            ajaxStop();
            alert('Error: ' + e);
        }
    });
}

