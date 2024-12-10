/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

$.ajax(
        {
            type: 'get',
            url: 'getBackground',
            success: function (result) {

                console.log("image name::::::::" + result);
                result = result.replace('\\', '/');

                var demo = JSON.parse(result);
                var img1 = demo.background;

                if (img1 == null)
                {

                    $("body").css("background-color", "white");
                } else

                {

                    img1 = img1.replace('//', '/');

                    console.log("img1:::::::" + img1);
                    var filename = img1.replace(/^.*[\\\/]/, '')
                    // alert(filename);
                    var URL = $("#ssThemesURL").val()+"/Themes/"+filename;

                    $("body").css("background", "url('" + URL + "') no-repeat center center fixed");
                    $("body").css("-webkit-background-size", "cover");
                    $("body").css("-moz-background-size", "cover");
                    $("body").css("-o-background-size", "cover");
                    $("body").css("background-size", "cover");
                }
            },
            error: function (result) {
                console.log(e);
                sessionTimeout(result);
            }
        });



