/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


(function ($) {
    $.fn.ovaltooltip = function (text) {
    };
}(jQuery));


function closeTooltip() {
    $(".tool-tip-position").remove();
}

function hideOvalTooltip() {
    $(".tool-tip-position").hide();
}


function DoTrick(inputid, tooltiptext)
{
    closeTooltip();
    var $input = $("#" + inputid);
    var $textdiv = $("<div/>")
            .addClass("visionLoginDialogText")
            .text(tooltiptext);
    var $ovaldiv = $("<div/>")
            .addClass("visionLoginOval");
    var $trianglediv = $("<div/>")
            .addClass("visionLoginInvertedTriangle");
    var $parentdiv = $("<div onclick='hideOvalTooltip();'/>")
            .addClass("tool-tip-position");
    //  $parentdiv=$(".tool-tip-position");
    $parentdiv.append($ovaldiv.append($textdiv));
    $parentdiv.append($trianglediv);
    $parentdiv.insertBefore($input);

    $parentdiv.css('position', 'absolute');
    var x = $parentdiv.position();
    //alert("Top: " + x.top + " Left: " + x.left);
    $parentdiv.offset({top: x.top - 75, left: x.left + 370});

    console.log($textdiv.height());
    if (parseInt($textdiv.height()) > 40)
    {
        var newwidth = parseInt($(".visionLoginOval").css('width')) + ($textdiv.height() + 500);
        console.log($(".visionLoginOval").css('width') + "-->" + newwidth);

        $(".visionLoginOval").css('width', "220px");
        console.log($textdiv.height());
        x = $parentdiv.position();
        //$parentdiv.offset({top: x.top + ($textdiv.height() - 100), left: x.left});
        $parentdiv.offset({top: x.top - 20, left: x.left + 20});
    }
//    $parentdiv.addEventListener('click',$(this).hide(), false);
}


function showTooltip(tbid, tooltiptext) {
    // alert('sdsd'); 
    DoTrick(tbid, tooltiptext);
//                $(".dialog-text").remove();
//                $(".oval").append("<div class='dialog-text'>" + $("#input_text").val() + "</div></div>");

}

