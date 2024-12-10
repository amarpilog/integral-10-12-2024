/**
 * @license MIT
 * @license http://opensource.org/licenses/MIT Massachusetts Institute of Technology
 * @copyright 2014 Patric Gutersohn
 * @author Patric Gutersohn
 * @example index.html BDT in action.
 * @link http://bdt.gutersohn.biz Documentation
 * @version 1.0.0
 *
 * @summary BDT - Bootstrap Data Tables
 * @description sorting, paginating and search for bootstrap tables
 */
var obj = null;
(function($) {
    "use strict";

    /**
     * @type {number}
     */
    var actualPage = 1;
    /**
     * @type {number}
     */
     var pageno = 1;
    var pageCount = 0;
    /**
     * @type {number}
     */
    var pageRowCount = 0;
    /**
     * @type {string}
     */
    var pages = '';
    /**
     * @type {object}
     */

    /**
     * @type {boolean}
     */
    var activeSearch = false;
    /**
     * @type {string}
     */
    var arrowUp = '';
    /**
     * @type {string}
     */
    var arrowDown = '';


// ROW COUNT
    var rowFilterCount = 0;

    $.fn.bdt = function(options, callback) {

        var settings = $.extend({
            pageRowCount: 10,
            arrowDown: 'fa-angle-down',
            arrowUp: 'fa-angle-up'
        }, options);

        /**
         * @type {object}
         */
        var tableBody = null;

        return this.each(function() {
            obj = $(this).addClass('bdt');
            tableBody = obj.find("tbody");
            pageRowCount = settings.pageRowCount;
            arrowDown = settings.arrowDown;
            arrowUp = settings.arrowUp;

            /**
             * search input field
             */
            obj.before(
                    $('<form/>')
                    .addClass('pull-left')
                    .attr('role', 'form')
                    .append(
                            $('<div/>')
                            .addClass('form-group')

                            )


                    );

            /**
             * select field for changing row per page
             */
            obj.before(
                    $('<div/>')
                    .attr('id', 'table-footer')
                    .append(
                            $('<div/>')
                            .addClass('pull-left')
                            .append(
                                    $('<form/>')
                                    .addClass('form-horizontal')
                                    .attr('id', 'page-rows-form')
                                    .append($('<label/>')
                                            .addClass('pull-left control-label')
                                            .text('Entries per Page:')
                                            )
                                    .append(
                                            $('<div/>')
                                            .addClass('pull-left')
                                            .append(
                                                    $('<select/>')
                                                    .addClass('form-control')
                                                    .append(
                                                            $('<option>', {
                                                                value: 10,
                                                                text: 10,
                                                                selected: 'selected'
                                                            })
                                                            )
                                                    .append(
                                                            $('<option>', {
                                                                value: 50,
                                                                text: 50,
                                                            })
                                                            )
                                                    .append(
                                                            $('<option>', {
                                                                value: 100,
                                                                text: 100,
                                                            })
                                                            )
                                                    .append(
                                                            $('<option>', {
                                                                value: 250,
                                                                text: 250
                                                            })
                                                            )
                                                    .append(
                                                            $('<option>', {
                                                                value: 500,
                                                                text: 500
                                                            })
                                                            )
                                                    .append(
                                                            $('<option>', {
                                                                value: 750,
                                                                text: 750
                                                            })
                                                            )
                                                    .append(
                                                            $('<option>', {
                                                                value: 1000,
                                                                text: 1000
                                                            })
                                                            )
                                                    )
                                            )
                                    )
                            )
                    );

            if (tableBody.children('tr:visible').length > pageRowCount) {
                setPageCount(tableBody);
                addPages();
                paginate(tableBody, actualPage);
            }

            searchTable(tableBody);
            sortColumn(obj, tableBody);

            $('body').on('click', '.pagination li', function(event) {
                var listItem;

                if ($(event.target).is("a")) {
                    listItem = $(event.target).parent();
                } else {
                    listItem = $(event.target).parent().parent();
                }

                var page = listItem.data('page');

                if (!listItem.hasClass("disabled") && !listItem.hasClass("active")) {
                    paginate(tableBody, page);
                }
            });

            $('#page-rows-form').on('change', function() {
                var options = $(this).find('select');
                pageRowCount = options.val();

                setPageCount(tableBody);
                addPages();
                paginate(tableBody, 1);
            });

        });

        /**
         * the main part of this function is out of this thread http://stackoverflow.com/questions/3160277/jquery-table-sort
         * @author James Padolsey http://james.padolsey.com
         * @link http://jsfiddle.net/spetnik/gFzCk/1953/
         * @param obj
         */
        function sortColumn(obj) {
            var table = obj;
            var oldIndex = 0;

            obj
                    .find('thead th')
                    .wrapInner('<span class="sort-element"/>')
                    .append(
                            $('<span/>')
                            .addClass('sort-icon fa')
                            )
                    .each(function() {

                        var th = $(this);
                        var thIndex = th.index();
                        var inverse = false;
                        var addOrRemove = true;

                        th.click(function() {

                            if ($(this).find('.sort-icon').hasClass(arrowDown)) {
                                $(this)
                                        .find('.sort-icon')
                                        .removeClass(arrowDown)
                                        .addClass(arrowUp);

                            } else {
                                $(this)
                                        .find('.sort-icon')
                                        .removeClass(arrowUp)
                                        .addClass(arrowDown);
                            }

                            if (oldIndex != thIndex) {
                                obj.find('.sort-icon').removeClass(arrowDown);
                                obj.find('.sort-icon').removeClass(arrowUp);

                                $(this)
                                        .find('.sort-icon')
                                        .toggleClass(arrowDown, addOrRemove);
                            }

                            table.find('td').filter(function() {
//alert('--------------');

                                return $(this).index() === thIndex;

                            }).sortElements(function(a, b) {

                                return $.text([a]) > $.text([b]) ?
                                        inverse ? -1 : 1
                                        : inverse ? 1 : -1;

                            }, function() {

                                // parentNode is the element we want to move
                                return this.parentNode;

                            });

                            inverse = !inverse;
                            oldIndex = thIndex;
                        });

                    });
        }

        /**
         * create li elements for pages
         */

        var pagesArray = '';
        pagesArray = pageCount;
       
      
        function addPages() {
            $('#table-nav').remove();
            pages = $('<ul/>');



          //  console.log(pagesArray);

            $.each(new Array(pageCount), function(index) {
                var additonalClass = '';
                var page = $();

                if ((index + 1) == 1) {
                    additonalClass = 'active';
                }

                pages
                        .append($('<li/>')
                                .addClass(additonalClass)
                                .data('page', (index + 1))
                                .append(
                                        $('<a/>')
                                        .text(index + 1)
                                        )
                                );
            });

            /**
             * pagination, with pages and previous and next link
             */
            $('#table-footer')
                    .addClass('row')
                    .append(
                            $('<nav/>')
                            .addClass('pull-right')
                            .attr('id', 'table-nav')
                            .append(
                                    pages
                                    .addClass('pagination pull-right')
                                    .prepend(
                                            $('<li id="prev_pg"/>')
                                            .addClass('disabled')
                                            .data('page', 'previous')
                                            .append(
                                                    $('<a href="#"/>')
                                                    .append(
                                                            $('<span/>')
                                                            .attr('aria-hidden', 'true')
                                                            .html('&laquo;')
                                                            )
                                                    .append(
                                                            $('<span/>')
                                                            .addClass('sr-only')
                                                            .text('Previous')
                                                            )
                                                    )
                                            )


                                  

   .append(
                                            $('<li />')
                                            .addClass('disabled')
                                            .data('page', 'next')
                                            .append(
                                                    $('<a href="#"   />')
                                                    .append(
                                                            $('<span/>')
                                                            .attr('aria-hidden', 'true')
                                                            .html('&raquo;')
                                                            )
                                                    .append(
                                                            $('<span/>')
                                                            .addClass('sr-only')
                                                            .text('Next')
                                                            )
                                                    )
                                            )




                              
                                    )
                            );

        }

  


        /**
         *
         * @param tableBody
         */




        function searchTable(tableBody) {
            $("#vnamehead").on("keyup", function() {
                $.each(tableBody.find("tr"), function() {

                    var text = $(this)
                            .text()
                            .replace(/ /g, '')
                            .replace(/(\r\n|\n|\r)/gm, "");

                    var searchTerm = $("#vnamehead").val();

                    if (text.toLowerCase().indexOf(searchTerm.toLowerCase()) == -1) {
                        $(this)
                                .hide()
                                .removeClass('search-item');
                    } else {
                        $(this)
                                .show()
                                .addClass('search-item');
                    }

                    if (searchTerm != '') {
                        activeSearch = true;
                    } else {
                        activeSearch = false;
                    }
                });

                setPageCount(tableBody);
                addPages();
                paginate(tableBody, 1);
            });
            $("#instancehead").on("keyup", function() {
                $.each(tableBody.find("tr"), function() {

                    var text = $(this)
                            .text()
                            .replace(/ /g, '')
                            .replace(/(\r\n|\n|\r)/gm, "");

                    var searchTerm = $("#instancehead").val();

                    if (text.toLowerCase().indexOf(searchTerm.toLowerCase()) == -1) {
                        $(this)
                                .hide()
                                .removeClass('search-item');
                    } else {
                        $(this)
                                .show()
                                .addClass('search-item');
                    }

                    if (searchTerm != '') {
                        activeSearch = true;
                    } else {
                        activeSearch = false;
                    }
                });

                setPageCount(tableBody);
                addPages();
                paginate(tableBody, 1);
            });
        }
        /**
         *
         * @param tableBody
         */
        function setPageCount(tableBody) {
            if (activeSearch) {
                pageCount = Math.round(tableBody.children('.search-item').length / pageRowCount);
            } else {
                //alert('eneterdd set page');
                pageCount = Math.round(tableBody.children('tr:visible').length / pageRowCount);
            }

            if (pageCount == 0) {
                pageCount = 1;
            }
        }
        /**
         *
         * @param tableBody
         * @param page
         */
        function paginate(tableBody, page) {
            // alert("===paginate="+page);
            if (page == 'next') {
                page = actualPage + 1;
            } else if (page == 'previous') {
                page = actualPage - 1;
            }

            actualPage = page;

            var rows = (activeSearch ? tableBody.find(".search-item") : tableBody.find("tr").css("display", "tabe-row"));
            var endRow = (pageRowCount * page);
            var startRow = (endRow - pageRowCount);
            var pagination = $('.pagination');

            rows
                    .hide();

            rows
                    .slice(startRow, endRow)
                    .show();

            pagination
                    .find('li')
                    .removeClass('active disabled');

            pagination
                    .find('li:eq(' + page + ')')
                    .addClass('active');

            if (page == 1) {
                pagination
                        .find('li:first')
                        .addClass('disabled');

            } else if (page == pageCount) {
                pagination
                        .find('li:last')
                        .addClass('disabled');
            }
        }
        function onKeyup() {
            //  alert('11');


            //  vendorhead 

            var $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('0').text());
                //cval=cval.toUpperCase;
                var cval1 = $.trim($(this).find('td').eq('3').text());
                // cval1 = cval1.toUpperCase();

                return  cval.indexOf(cval1) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('1').text());
                //var cval1 = $.trim($(this).find('td').eq('3').text());
                return  cval.indexOf($("#vnamehead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('2').text());
                return  cval.indexOf($("#suppliernohead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('3').text());
                return  cval.indexOf($("#instancehead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('4').text());
                return  cval.indexOf($("#acghead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('5').text());
                return  cval.indexOf($("#pohead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('6').text());
                return  cval.indexOf($("#cchead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('7').text());
                return  cval.indexOf($("#erphead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('8').text());
                return  cval.indexOf($("#createdbyhead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('9').text());
                return  cval.indexOf($("#cdatehead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('10').text());
                return  cval.indexOf($("#editedbyhead").val()) != -1;
            }).show();
            $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                var cval = $.trim($(this).find('td').eq('11').text());
                return  cval.indexOf($("#edatehead").val()) != -1;
            }).show();

            if ($("#vendorhead").val() != "") {
                // alert('entered vname head');
                var $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('0').text());
                    return  cval.indexOf($("#vendorhead").val()) == -1;
                }).hide();
            }
            if ($("#vnamehead").val() != "") {
                // alert('entered vname head');
                var $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('1').text());
                    return  cval.indexOf($("#vnamehead").val()) == -1;
                }).hide();



            }
            if ($("#suppliernohead").val() != "") {
                // alert('entered vname head');
                var $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('2').text());
                    return  cval.indexOf($("#suppliernohead").val()) == -1;
                }).hide();
            }
            if ($("#instancehead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('3').text());
                    return  cval.indexOf($("#instancehead").val()) == -1;
                }).hide();
            }
            if ($("#acghead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('4').text());
                    return  cval.indexOf($("#acghead").val()) == -1;
                }).hide();
            }
            if ($("#pohead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('5').text());
                    return  cval.indexOf($("#pohead").val()) == -1;
                }).hide();
            }
            if ($("#cchead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('6').text());
                    return  cval.indexOf($("#cchead").val()) == -1;
                }).hide();
            }
            if ($("#erphead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('7').text());
                    return  cval.indexOf($("#erphead").val()) == -1;
                }).hide();
            }
            if ($("#createdbyhead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('8').text());
                    return  cval.indexOf($("#createdbyhead").val()) == -1;
                }).hide();
            }
            if ($("#cdatehead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('9').text());
                    return  cval.indexOf($("#cdatehead").val()) == -1;
                }).hide();
            }
            if ($("#editedbyhead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('10').text());
                    return  cval.indexOf($("#editedbyhead").val()) == -1;
                }).hide();
            }
            if ($("#editedbyhead").val() != "") {
                //    alert('entered instance head');
                $rowsNo = $('#bootstrap-table tbody tr').filter(function() {
                    var cval = $.trim($(this).find('td').eq('11').text());
                    return  cval.indexOf($("#edatehead").val()) == -1;
                }).hide();
            }
///alert('keyup count:'+$('#bootstrap-table tbody tr:visible').length);
            tableBody = $('#bootstrap-table tbody tr:visible').find("tbody").css("display", "table-row");
//alert(tableBody);
//setPageCount(tableBody);
            addPages();
            paginate(tableBody, 1);
        }

    }
    
    function previous(pagecountval, pageno) {
        //console.log('entered previous');
          alert(''+pagecountval+" "+pageno);  
            if (pagecountval == 1) {
                $("#prev_pg").css("disabled", "true");
                $("#next_pg").css("disabled", "true");
            }

            if (pageno < pagecountval) {
                pageno = pageno - 1;
            }
            else {

            }


        }
    function next(pagecountval) {
            if (pagecountval == 1) {
                $("#prev_pg").css("disabled", "true");
                $("#next_pg").css("disabled", "true");
            }
            else {
                if (pageno <= pagecountval) {
                    pageno = pageno + 1;
                }
            }




        }
}(jQuery));




