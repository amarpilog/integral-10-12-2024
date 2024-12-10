/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
function onfocuscc(tabid) {
    alert(tabid);
}
function addInlineRow() {
    var rowCount = $('#paymentdata tr').length;
    $("#paymentdata")
            .append('<tr on id=' + rowCount + '><td id=a' + rowCount + '><input type="checkbox" ></td><td id=b' + rowCount + '><input type="text" /><td id=c' + rowCount + '><input type="text" /></td><td id=d' + rowCount + '><input type="text"/></td><td id=e' + rowCount + '><input type="text"/></td><td id=f' + rowCount + '><input type="text"/></td><td id=g' + rowCount + '><input type="text"/></td><td id=h' + rowCount + '><input type="text"/></td><td id=i' + rowCount + '><input type="text"/></td><td id=j' + rowCount + '><input type="text"/></td><td id=j' + rowCount + '><input type="text"/></td><td id=k' + rowCount + '><input type="text"/></td></tr>');
}
function deleteInlineRow() {
    $("#paymentdata").find('input:checkbox:checked').parents("tr").remove();
}

function getTableDataV2() {
    var table = document.getElementById("#paymentdata");
    for (var i = 0, row; row < table.rows[i]; i++) {
        //iterate through rows
        //rows would be accessed using the "row" variable assigned in the for loop
        for (var j = 0, col; col < row.cells[j]; j++) {

            console.log($(row.cells[j]).find(":input[type=text]").val());



            ///
            //
            //iterate through columns
            //columns would be accessed using the "col" variable assigned in the for loop
        }
    }

}

function getTableData1() {
    var data = [];
    $("#paymentdata").find('tr id').each(function(rowIndex, r) {
        var cols = [];
        $(this).find('th,td id').each(function(colIndex, c) {
            console.log("Row Index col Index:" + rowIndex + "," + colIndex);
        });
        data.push(cols);
    });
    return data;
}

function getTableData() {
    var data = [];
    $("#paymentdata").find('tr').each(function(rowIndex, r) {
        alert("row index1:" + rowIndex);
        var cols = [];
        $(this).find('tr,td').each(function(colIndex, c) {

            $(tr).find('td:eq(0)').find('input').val(); // Task No.
            //alert($(colIndex).find(":input[type=text]").val());
            cols.push($(tr).find('td:eq(0)').find('input').val());
        });
        data.push(cols);
    });
    console.log(data);
}


function getTableData12() {
    var data = [];
    $("#paymentdata").find('tr').each(function(rowIndex, r) {
        var cols = [];

    });

}

function getTableData3() {
    var TableData = new Array();
    var data = [];

    $('#paymentdata tr').each(function(row, tr) {
        // $(this).attr('name')
        //alert("row:"+row+" tr:"+tr);
        //alert($(tr).find('td:eq(1)').find('input').val());
        alert($(tr).find('id').val());
        var cols = [];

        TableData = TableData
                + $(tr).find('td:eq(1)').find('input').val() + ' '  // Date
                + $(tr).find('td:eq(2)').find('input').val() + ' '  // Description
                + $(tr).find('td:eq(3)').find('input').val() + ' '  // Task
                + $(tr).find('td:eq(4)').find('input').val() + ' '  // Task
                + $(tr).find('td:eq(5)').find('input').val() + ' '  // Task
                + $(tr).find('td:eq(6)').find('input').val() + ' '  // Task
                + $(tr).find('td:eq(7)').find('input').val() + ' '  // Task
                + $(tr).find('td:eq(8)').find('input').val() + ' '  // Task
                + $(tr).find('td:eq(9)').find('input').val() + ' '  // Task
                + $(tr).find('td:eq(10)').find('input').val() + ' '  // Task
                + $(tr).find('td:eq(11)').find('input').val() + ' '  // Task
                + '\n';


    });



}


function getTableData4() {
    var TableData = new Array();
    var data = [];

    $('#paymentdata tr').each(function(row, tr) {
        // $(this).attr('name')
        //alert("row:"+row+" tr:"+tr);
        //alert($(tr).find('td:eq(1)').find('input').val());

        var t1;
        alert($(tr).find('id').val());
        var cols = [];
        t1.IFCS = $(tr).find('td:eq(1)').find('input').val();


        cols.push($(tr).find('td:eq(1)').find('input').val());
        cols.push($(tr).find('td:eq(2)').find('input').val());
        cols.push($(tr).find('td:eq(3)').find('input').val());
        cols.push($(tr).find('td:eq(4)').find('input').val());
        cols.push($(tr).find('td:eq(5)').find('input').val());
        cols.push($(tr).find('td:eq(6)').find('input').val());
        cols.push($(tr).find('td:eq(7)').find('input').val());
        cols.push($(tr).find('td:eq(8)').find('input').val());
        cols.push($(tr).find('td:eq(9)').find('input').val());
        cols.push($(tr).find('td:eq(10)').find('input').val());
        cols.push($(tr).find('td:eq(11)').find('input').val());
        data.push(cols);
    });

    alert(data);
    console.log(data);




}

function getTableData5() {
    
   // alert("supcode:"+supcode);
   // alert("locatcode:"+locatcode);
    
    
    var TableData = new Array();
    var data = [];
    var t1 = {};
    t1.supcode = $('#vendorCode').val();
    t1.locatcode = $('#locatcode').val();
    t1.compcode=$('#comp_code').val();
    t1.account_group  =$('#account_group').val();      
    t1.purch_org=$('#purch_org').val();
    t1.ptdata = [];

    $('#paymentdata tr').each(function(row, tr) {
        t1.ptdata.push(
                {
                    IFCS: $(tr).find('td:eq(1)').find('input').val(),
                    bankacc: $(tr).find('td:eq(2)').find('input').val(),
                    bankname: $(tr).find('td:eq(3)').find('input').val(),
                    bankaddress: $(tr).find('td:eq(4)').find('input').val(),
                    bankcity: $(tr).find('td:eq(5)').find('input').val(),
                    region: $(tr).find('td:eq(6)').find('input').val(),
                    country: $(tr).find('td:eq(7)').find('input').val(),
                    regioncode: $(tr).find('td:eq(8)').find('input').val(),
                    swiftcode: $(tr).find('td:eq(9)').find('input').val(),
                    partnerbanktype: $(tr).find('td:eq(10)').find('input').val(),
                    IBANCODE: $(tr).find('td:eq(11)').find('input').val()
                }
        );
    });

    $.ajax({
        type: "get",
        url: "TestServlet",
        data: {jsondata: JSON.stringify(t1)},
        traditional: true,
        dataType: 'text/html',
        cache: false,
        success: function(response) {
            alert("=response====" + response);
            document.getElementById("tierb").innerHTML = response;


        },
        error: function(e) {
             sessionTimeout(e);
        }

    });
}