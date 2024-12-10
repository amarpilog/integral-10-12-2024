/*omi*/
function isEmpty(el) {
    return !$.trim(el.html())
}
$(document).ready(function () {
    $(".sidebarIconMenu").click(function (e) {
        $(".visionSidebarMenuItem").toggle();
        e.stopPropagation();
        if ($(".visionSidebarMenuItem").is(":visible")) {
            $(".visionBodyContentInner").addClass("visiondisabledDiv");
        } else {
            $(".visionBodyContentInner").removeClass("visiondisabledDiv");
        }
        var sideMenuUlCode = $("#sideMenuULMainId").text();
        if (sideMenuUlCode != null && sideMenuUlCode != '') {
            sideMenuUlCode = sideMenuUlCode.replace(/(\r\n|\n|\r)/gm, "");
            sideMenuUlCode = sideMenuUlCode.replace(/\s/g, "");
        }
        if (!(sideMenuUlCode != null && sideMenuUlCode != '')) {
            $.ajax({
                type: "post",
                traditional: true,
                dataType: 'html',
                cache: false,
                url: "getSideMenuData",
                success: function (response) {
                    if (response != null)
                    {
                        $("#visionSidebarMenuItem").html(response);
                        $("#sideMenuULMainId").menu();
                        var children = $('#sideMenuULMainId li a').filter(function () {
                            return $(this).nextAll().length > 0
                        });
                        $('<span class="visionSidebarMenuArrowIcon"><img src="images/icon-right.png"></img></span>').insertAfter(children);
                        $('#sideMenuULMainId .visionSidebarMenuArrowIcon').on('click touch', function (e) {
                            $(this).closest('li').siblings().find('ul').hide();
                            $(this).next().show();
                        });
                    }
                    $("#sideMenuULMainId").mouseleave(function () {
                        $("#sideMenuULMainId").menu('collapseAll');
                        $(".sideMenuUlClass").css('display', 'none');
                    });
                    $(".sideMenuUlClass").mouseleave(function () {
                        $("#sideMenuULMainId").menu('collapseAll');
                        $(this).css('display', 'none');
                    });
                }, //end success
                error: function (e) {
                    sessionTimeout(e);
                }
            });
        }//end if
    });//end click function
    var sideMenuUlCode = $("#sideMenuULMainId").text();
    if (sideMenuUlCode != null && sideMenuUlCode != '') {
        sideMenuUlCode = sideMenuUlCode.replace(/(\r\n|\n|\r)/gm, "");
        sideMenuUlCode = sideMenuUlCode.replace(/\s/g, "");
    }
    if ((sideMenuUlCode != null && sideMenuUlCode != '')) {
        $("#sideMenuULMainId").menu();
        var children = $('#sideMenuULMainId li a').filter(function () {
            return $(this).nextAll().length > 0
        });
        $('<span class="visionSidebarMenuArrowIcon"><img src="images/icon-right.png"></img></span>').insertAfter(children);
        $('#sideMenuULMainId .visionSidebarMenuArrowIcon').on('click touch', function (e) {
            $(this).closest('li').siblings().find('ul').hide();
            $(this).next().show();
//                 $("#sideMenuULMainId").show();
        });
    }
    $("#sideMenuULMainId").mouseleave(function () {
        $("#sideMenuULMainId").menu('collapseAll');
        $(".sideMenuUlClass").css('display', 'none');
    });

    $(".sideMenuUlClass").mouseleave(function () {
        $("#sideMenuULMainId").menu('collapseAll');
        $(this).css('display', 'none');
    });
    var windowWidth = parseInt($(window).width());
    if (windowWidth >= 1285) {
        $(document).click(function () {
            $(".visionSidebarMenuItem").hide();
            $("#sideMenuULMainId").menu('collapseAll');
            $(".visionBodyContentInner").removeClass("visiondisabledDiv");
        });
    }

});//end
