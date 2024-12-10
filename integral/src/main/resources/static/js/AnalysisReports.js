/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



/*Used for runAnalysis in charts*/
function runAnalysis(gridId) {
    $.ajax({
        type: "post",
        traditional: true,
        dataType: 'text',
        cache: false,
        url: "getGridAnalysis",
        data: {
            gridid: gridId
        },
        success: function (response) {
            var baskettype = "";
            var analysisArray = JSON.parse(response);
//                            $("#analysisForm").remove();
            var dilogHtml = "<h3>Choose Analysis Type</h3><div class='analysisBox'>";

            //            var dilogHtml = "<h3>Choose Analysis Type</h3><form id='analysisForm' target='_blank' action='reporTabs' method='POST' style='display:none'>"
            //                    + "<input type='hidden' id='reportTabId' name='tabid'/>"
            //                    + "<input type='hidden' id='baskettype' name='baskettype'/>"
            //                    + "</form><div class='analysisBox'>";


            for (var i = 0; i < analysisArray.length; i++)
            {
                if (i == 0) {
                    baskettype = analysisArray[i].baskettype;
                    dilogHtml = dilogHtml + "<input type='radio' name='analysistype' value=" + analysisArray[i].analysisTab + " class='analysisType' checked='true' name='analysistype'><span class='analysisLabel'>" + analysisArray[i].analysisType + "</span>";
                } else {
                    dilogHtml = dilogHtml + "<br/><input type='radio' name='analysistype' value=" + analysisArray[i].analysisTab + " class='analysisType' name='analysistype'><span class='analysisLabel'>" + analysisArray[i].analysisType + "</span>";
                }
            }
            dilogHtml = dilogHtml + "</div>";
            $("#dialog").html(dilogHtml);
            $("#dialog").dialog({
                title: (labelObject['Analysis'] != null ? labelObject['Analysis'] : 'Analysis'),
                modal: true,
                height: 230,
                minWidth: 450,
                maxWidth: 'auto',
                fluid: true,
                buttons: [{
                        text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                        click: function () {
//                                            $("#analysisForm").remove();
//                                                $("#analysisForm").empty();
                            var analysis = $("input[type='radio'][name='analysistype']:checked").val();
                            $("#tabid").val('');
                             $("[name=tabid]").val(analysis);
                            $("#analysisForm").attr("action", 'reporTabs');
                            $("#analysisForm").attr("method", 'POST');
                           // var tabid = "<input type='hidden' id='tabid' name='tabid' value='" + analysis + "'/>";
                           // var baskettype = "<input type='hidden' id='baskettype' name='baskettype' value='" + baskettype + "'/>";
                            //$("#analysisForm").append(tabid);
                            //$("#analysisForm").append(baskettype);
//                            $("#tabid").val(analysis);
                            $("#baskettype").val(baskettype);
//                                            $("#tabid").val(analysis);
//                                            $("#baskettype").val(baskettype);
                            //  charttabid(analysis,baskettype);
                            $("#analysisForm").submit();
                            // $("#analysisForm").remove();
                            $(this).html("");
                            $(this).dialog("close");



                        }
                    }],
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
            $("input[type='radio'][name='analysistype']").change(function () {
                // $("#analysisForm").empty();
                var analysis = $("input[type='radio'][name='analysistype']:checked").val();
 
                $("#analysisForm").attr("action", 'reporTabs');
                $("#analysisForm").attr("method", 'POST');
                $("#tabid").val('');
                $("[name=tabid]").val(analysis);
                $("#baskettype").val('');
                $("#baskettype").val(baskettype);

               // var tabid = "<input type='hidden' id='tabid' name='tabid' value='" + analysis + "'/>";
              //  var baskettype = "<input type='hidden' id='baskettype' name='baskettype' value='" + baskettype + "'/>";
               // $("#analysisForm").append(tabid);
               // $("#analysisForm").append(baskettype);
                try {
                    var formhtml = $("#analysisForm").html();
                } catch (e) {
                }
                $("#analysisForm").submit();
            });
        },
        error: function (e) {
            sessionTimeout(e);
        }

    });




}

function charttabid(analysis, baskettype) {
    $.ajax({
        type: "GET",
        traditional: true,
        dataType: 'text',
        cache: false,
        url: "reporTabs",
        data: {
            analysis: analysis,
            baskettype: baskettype
        },
        success: function (response) {

        },
        error: function (e) {
            sessionTimeout(e);
        }

    });
}







function chartid(gridId) {
    $("#dialog").html("dialogSplitMessage");

    $("#dialog").dialog({
        title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
        modal: true,
        height: 300,
        minHeight: 'auto',
        minWidth: 900,
        maxWidth: 'auto',
        fluid: true,
        buttons: [{
                text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
                click: function () {
                    $(this).html("");
                    $(this).dialog("close");
                    $(this).dialog("destroy");
                }
            }],
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

function customizeChart(currentchartid) {
    try {
        //fetch chart filter data

        $.ajax({
            type: "post",
            traditional: true,
            dataType: 'JSON',
            cache: false,
            url: "getChartFilterInfo",
            data: {
                chartid: currentchartid
            },
            success: function (response) {

                console.log(JSON.stringify(response));
                var tableString = "<table id='chartEditTbl' class='visionChartEditCover'> <thead><tr class='header'>";
                tableString = tableString + "<th class='tg-vro8'>${labelobj['Parameter'] != null ? labelobj['Parameter'] : 'Parameter'}</th>";
                tableString = tableString + "<th class='tg-vro8'>${labelobj['Operator'] != null ? labelobj['Operator'] : 'Operator'}</th>";
                tableString = tableString + "<th class='tg-vro8'>${labelobj['Value'] != null ? labelobj['Value'] : 'Value'}</th>";
                tableString = tableString + "</tr><tbody>";
                for (var i = 0; i < response.length; i++)
                {

                    if (response[i].defaultFlag == 'Y') {
                        tableString = tableString
                                + "<tr id='tr" + i + "'>"
                                + "<td id='tdcol" + i + "' data-colname='" + response[i].colname + "'>" + response[i].label + "</td>";
                    } else {
                        tableString = tableString
                                + "<tr id='tr" + i + "' style='display:none;'>"
                                + "<td id='tdcol" + i + "' data-colname='" + response[i].colname + "'>" + response[i].label + "</td>";
                    }
                    tableString = tableString
                            + "<td id='tdoper" + i + "'>" + response[i].operators + "</td>"
                            + "<td id='tdval" + i + "'>"
                            + "<input type='text' id='chart_flt_" + i + "' value='" + response[i].filterVal + "' class='paramtd_normal'/>";
                    //+ "<img src='images/search_icon_color_2.png' class='visionReportsTableImage'>";

                    if (response[i].ddwid != null && response[i].ddwid != 'null') {
                        tableString = tableString
                                + " <img id=\"tbddw" + i + "\" class=\"srch_ddw\" onclick=\"populateDropDown('" + response[i].ddwid + "','chart_flt_" + i + "'," + i + ")\" src=\"images/search_icon_color_2.png\">";
                    }
                    tableString = tableString + "</td>"
                            + "</tr>";
                }

                tableString = tableString + "</tbody></table>";

                $("#dialog1").html("");
                $("#dialog1").append(tableString);
                $("#dialog1").dialog({
                    title: 'Message',
                    modal: true,
                    height: 300,
                    minWidth: 500,
                    maxWidth: 'auto',
                    fluid: true,
                    buttons: {
                        Run: function () {


                            //                                        updateChartDetails(currentchartid);


                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                        },
                        Close: function () {
                            $(this).html("");
                            $(this).dialog("close");
                            $(this).dialog("destroy");
                        }

                    },
                    open: function () {
                        $(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
                        $(this).closest(".ui-dialog").find(".ui-button").eq(2).addClass("dialogno");
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
                sessionTimeout(e);
            }

        });



    } catch (e) {

    }

}
 