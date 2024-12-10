/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

$(document).ready(function() {
	//    $(".sidebar-dropdown").find(".dxpMaxMinMenuClass").on('mouseenter',function () {
	//    $(".sidebar-submenu").slideUp(300);
	//    var anchorTag = $(this).closest("a");
	//  
	//    if (anchorTag.parent().hasClass("active")) {
	//      $(".sidebar-dropdown").removeClass("active");
	//      anchorTag.parent().removeClass("active");
	//    }
	//    else {
	//      $(".sidebar-dropdown").removeClass("active");
	//      anchorTag.next(".sidebar-submenu").slideDown(300);
	//      anchorTag.parent().addClass("active");
	//    }
	//  });

	//    var timeOut = null;

	$(".sidebar-dropdown > a").on('click', function() {
		var $this = this;
		clearTimeout(timeOut);
		//        timeOut = setTimeout(function () {
		$(".sidebar-submenu").slideUp(100);
		if ($($this).parent().hasClass("active")) {
			$(".sidebar-dropdown").removeClass("active");
			$($this).parent().removeClass("active");
		} else {
			$(".sidebar-dropdown").removeClass("active");
			$($this).next(".sidebar-submenu").slideDown(100);
			$($this).parent().addClass("active");
		}
		//        }, 1000)
	});





	$(".level2dropdown >a").on('click', function() {
		var $this = this;
		clearTimeout(timeOut);
		//        timeOut = setTimeout(function () {
		$(".level2submenu").slideUp(100);
		if ($($this).parent().hasClass("active")) {
			$(".level2dropdown").removeClass("active");
			$($this).parent().removeClass("active");
		} else {
			$(".level2dropdown").removeClass("active");
			$($this).next(".level2submenu").slideDown(100);
			$($this).parent().addClass("active");
		}
		//        }, 1000)
	});
	$(".level3dropdown >a").on('click', function() {
		var $this = this;
		clearTimeout(timeOut);
		//        timeOut = setTimeout(function () {
		$(".level3submenu").slideUp(100);
		if ($($this).parent().hasClass("active")) {
			$(".level3dropdown").removeClass("active");
			$($this).parent().removeClass("active");
		} else {
			$(".level3dropdown").removeClass("active");
			$($this).next(".level3submenu").slideDown(100);
			$($this).parent().addClass("active");
		}
		//        }, 1000)
	});
	$(".level4dropdown >a").on('click', function() {
		var $this = this;
		clearTimeout(timeOut);
		//        timeOut = setTimeout(function () {
		$(".level4submenu").slideUp(100);
		if ($($this).parent().hasClass("active")) {
			$(".level4dropdown").removeClass("active");
			$($this).parent().removeClass("active");
		} else {
			$(".level4dropdown").removeClass("active");
			$($this).next(".level4submenu").slideDown(100);
			$($this).parent().addClass("active");
		}
		//        }, 1000)
	});


	//   $('.photos div').on('mouseenter mouseleave',function () {

	//     $(this).find('img.nocolor').togglefade('slow');
	// });

	//  $(".level2dropdown").find(".dxpMaxMinSecondClass").on('mouseenter',function () {
	//    $(".level2submenu").slideUp(300);
	//    var anchorTag = $(this).closest("a");
	//    if (anchorTag.parent().hasClass("active")) {
	//      $(".level2dropdown").removeClass("active");
	//      anchorTag.parent().removeClass("active");
	//    }
	//    else {
	//      $(".level2dropdown").removeClass("active");
	//      anchorTag.next(".level2submenu").slideDown(300);
	//      anchorTag.parent().addClass("active");
	//    }
	//  });

	$(".level3dropdown").find(".dxpMaxMinThirdClass").on('click', function() {
		var $this = this;
		clearTimeout(timeOut);
		//        timeOut = setTimeout(function () {
		$(".level3submenu").slideUp(100);
		var anchorTag = $($this).closest("a");
		if (anchorTag.parent().hasClass("active")) {
			$(".level3dropdown").removeClass("active");
			anchorTag.parent().removeClass("active");
		} else {
			$(".level3dropdown").removeClass("active");
			anchorTag.next(".level3submenu").slideDown(100);
			anchorTag.parent().addClass("active");
		}
		//        }, 1000)
	});

	$('#sidebar').on('mouseenter mouseleave', function() {

		$('#sidebar').toggleClass('toggled');
		$('.menuTitle').toggleClass("titleactive");
		// $(".sidebarTitle").toggle();

	});

});



/* Theme Change Javascript Starts Here by Santhosh */
function changeTheme(event) {
	let userLogin = localStorage['userName'];
	var element = $('body');
	element.toggleClass("dark-mode");
	if ($(element).hasClass('dark-mode')) {
		localStorage.setItem("localValue", userLogin.toUpperCase());
		localStorage.setItem("theme", "dark-mode");
		//        applyTheme(this, 'colorAsBgTheme', 'defalutBlackcoloredTheme');
		$("#themesShowClass").hide();
		$("#themeChangeSettingTitleId").text("Light Mode");
	} else {
		localStorage.setItem("localValue", "");
		localStorage.setItem("theme", "");
		$("#themeChangeSettingTitleId").text("Dark Mode");
		$("#themesShowClass").show();
		//         applyTheme(this, 'colorAsBgTheme', 'defalutWhitecoloredTheme');
	}
}
/* Theme Change Javascript Ends Here by Santhosh */

/* Theme Change Javascript Ends Here by Santhosh */
//$(document).ready(function () {
//    let userLogin = localStorage['userName']
//    let userLogintoUpper = userLogin.toUpperCase();
//    let localValue = localStorage['localValue']
//    if (userLogintoUpper === localValue) {
//        let element = $('body');
//        element.addClass("dark-mode");
//        if ($(element).hasClass('dark-mode')) {
//            console.log("alreadyhasclass");
//            $("#themeChangeSettingTitleId").text("Light Mode");
//
//        } else {
//            $('body').removeClass('dark-mode');
//            $("#themeChangeSettingTitleId").text("Dark Mode");
//        }
//    }
//});








/* Visualization  Change theme Images Starts Here by Santhosh */
function changeThemeVisualization() {
	var element = $('body');
	if ($(element).hasClass('dark-mode')) {
		$('.themeModeDark').each(function() {
			var a = $(this).attr('src');
			if (a.includes("dark")) {
				console.log("Already Has Dark Class");
			}
			else {
				$(this).attr('src', $(this).attr('src').replace(/images/i, "darkimages"));
			}
		});
	}
}



$(document).ready(function() {
	$('.dxpPageContent').scroll(function() {
		$("#top_arrow").show();
		$("#bottom_arrow").hide();
		var scroll = $('.dxpPageContent').scrollTop();
		console.log(scroll);
		if (scroll <= 0) {
			$("#top_arrow").hide();
			$("#bottom_arrow").show();
		} else {
			$("#top_arrow").show();
			$("#bottom_arrow").hide();
		}
	});
});





function scrollToBottom() {
	/*var heightscroll = $(document).height();
   $('.dxpPageContent').animate({ scrollTop: heightscroll }, 1200);*/


	var windowHeight = $(window).height();
	var documentHeight = $(document).height();
	var scrollTop = $(window).scrollTop();

	// Check if the user has scrolled to the bottom of the page
	if (scrollTop + windowHeight >= documentHeight) {
		var heightScroll = $('.visualizationDashboardView').height();
		var topScrollHeight = $("#mainintelliSenseSelectBoxId").height();
		$('.dxpPageContent').animate({ scrollTop: (documentHeight + heightScroll + topScrollHeight) }, 1200);
	}
}
$(window).scroll(function() {
	scrollToBottom();
});

function scrollToTop() {
	$('.dxpPageContent').animate({ scrollTop: 0 }, 600);
	$("#top_arrow").hide();
	$("#bottom_arrow").show();
}

/* Theme Change Javascript Ends Here by Santhosh */

function openSettingPannel(clickedIcon) {
	var clickedTitle = "";

	if (clickedIcon === 'settingdiv') {
		$('#settingsIcon').val('');
		$("#settingPannel").width(249);
		var clickedTitle = "Settings";
		$('#settingContentDiv').show();
		$('#helpContentDiv').hide();
		$('#userContentDiv').hide();
		$('#calendarContentDiv').hide();
		$("#settingPannel").addClass("settingPannelPopPannel");
		$("#settingPannel").removeClass("HelpPannelPopPannel");
		$("#settingPannel").addClass("userLoginPannelPopPannel");
		$('#clickedTitle').html(clickedTitle);
		$('#settingPannel').css('right', '0');
		$("#backgroundShadowDiv").show();
		$(".pannelTitle").show()
		$("#settingContentDiv").addClass("ToShowAllSetting");
		$('#settingsIcon').html("<img src='images/settingsWhiteSet.png' width='20px' class='themeModeDark'/>");
		console.log("Setting Pannel");
	}
	else if (clickedIcon == 'helpdiv') {
		$('#settingsIcon').val('');
		$("#settingPannel").width(249);
		var clickedTitle = "Help";
		$('#settingContentDiv').hide();
		$('#helpContentDiv').show();
		$('#userContentDiv').hide();
		$('#calendarContentDiv').hide();
		$("#settingPannel").addClass("helpPannelPopPannel");
		$("#settingPannel").removeClass("settingPannelPopPannel");
		$("#settingPannel").removeClass("userLoginPannelPopPannel");
		$('#clickedTitle').html(clickedTitle);
		$('#settingPannel').css('right', '0');
		$("#backgroundShadowDiv").show();
		$(".pannelTitle").show()
		$('#settingsIcon').html("<img src='images/helpWhiteSet.png' width='20px' class='themeModeDark'/>");
		console.log("Help Pannel");
		// $("#settingPannel").width(300);
	}
	else if (clickedIcon == 'useraccdiv') {
		$('#settingsIcon').val('');
		$("#settingPannel").width(249);
		let userLogin = localStorage['userName'];
		$('#settingContentDiv').hide();
		$('#helpContentDiv').hide();
		$('#userContentDiv').show();
		$('#calendarContentDiv').hide();
		$("#settingPannel").addClass("userLoginPannelPopPannel");
		$("#settingPannel").removeClass("helpPannelPopPannel");
		$("#settingPannel").removeClass("settingPannelPopPannel");
		$('#clickedTitle').html(userLogin);
		$('#settingPannel').css('right', '0');
		$("#backgroundShadowDiv").show();
		$(".pannelTitle").hide()
		console.log("userAccountDetails");
		$('#settingsIcon').html("<img src='images/userWhiteSet.png' width='20px' class='themeModeDark'/>");
		$('#imagePreview').css('background-image', 'url(' + localStorage['profile_imgStr'] + ')');
		$('#imagePreview').show();
		$("#imageUpload").off("change").on("change", function() {
			setuserProfileUpdateIcon(this);
		});
		// $("#settingPannel").width(300);
	}
	else if (clickedIcon == 'calendardiv') {
		$('#settingsIcon').val('');
		$("#settingPannel").width(300);
		var clickedTitle = "Calender";
		$('#settingContentDiv').hide();
		$('#helpContentDiv').hide();
		$('#userContentDiv').hide();
		$('#calendarContentDiv').show();
		$('#clickedTitle').html(clickedTitle);
		$('#settingPannel').css('right', '0');
		$("#backgroundShadowDiv").show();
		$(".pannelTitle").show()
		$('#settingsIcon').html("<img src='images/calendarWhiteSet.png' width='20px' class='themeModeDark'/>");
		showCustomScheduledCalendar();
		//   showEvents(selectFullDate);
		setTimeout(function() {
			//			showCustomScheduledCalendar(); 
			showEvents();
		}, 210);

	}
	if (clickedIcon == 'limitsIcon') {

		openLimitsPopOver(event);
	}
}
function closesettingPannel() {
	$("#settingPannel").width(0);
	$('#settingPannel').css('right', '-6px');
	$("#backgroundShadowDiv").hide();
}


function applyTheme($this, themeBackground, defaultColor, storedTheme = null) {
	$(".dxpLoginHeader").removeClass('lightthemeColors');
	var defaultColor = defaultColor;
	let userLogin = localStorage['userName'];
	let userLogintoUpper = userLogin.toUpperCase();
	let localValue = localStorage['localValue'];
	let themeItem = storedTheme || localStorage.getItem(`${userLogin}_headerTheme`);
	if (themeItem) {
		if (themeItem.startsWith("url(")) {
			$(".dxpLoginHeader").css("background", themeItem, "!important")
		} else {
			$(".dxpLoginHeader").css("background", themeItem, "!important");
		}
		$(".dxpLoginHeader").addClass('lightthemeColors');
	}
	if (themeBackground == 'colorAsBgTheme') {
		var currentEventBackgroundColor = $($this).children().css('background-color');
		$(".dxpLoginHeader").css("background", currentEventBackgroundColor);
		$(".dxpLoginHeader").addClass('lightthemeColors');
		try {
			//updateUserThemes("backgroundTheme", currentEventBackgroundColor)
		} catch (e) {

		}
	} else if (themeBackground == 'imageAsBgTheme') {
		var currentEventBackgroundImage = $($this).children().attr("src");
		$(".dxpLoginHeader").css('background-image', 'url(' + currentEventBackgroundImage + ')');
		$(".dxpLoginHeader").addClass('lightthemeColors');
		try {
			//updateUserThemes("backgroundTheme", 'url(' + currentEventBackgroundImage + ')')
		} catch (e) {

		}
	}
	if (themeBackground === "colorAsBgTheme" || themeBackground === "imageAsBgTheme") {
		let elementStyle = document.querySelector(".lightthemeColors");
		if (elementStyle && themeBackground === "colorAsBgTheme") {
			let style = $(elementStyle).attr("style");
			if (style) {
				const rgba = style.split(":")[1].trim().replace(/;$/, '');
				localStorage.setItem(`${userLogin}_headerTheme`, rgba);
			}
		} else if (elementStyle && themeBackground === "imageAsBgTheme") {
			let style = $(elementStyle).attr("style");
			if (style) {
				const url = style.split('url(')[1].split(')')[0];
				localStorage.setItem(`${userLogin}_headerTheme`, 'url(' + url + ')');
			}
		}
		//  applyImageColorFilter(themeBackground);
	}

}



/*function applyTheme(getThemename) {
	var getTargetThemeName = getThemename;
	if (getTargetThemeName == "defaultColor") {
		$('body').addClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "primaryColor") {
		$('body').addClass("primaryColor");
		$('body').removeClass("defaultColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "secondaryColor") {
		$('body').addClass("secondaryColor");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "basicColor") {
		$('body').addClass("basicColor");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "darkedColor") {
		$('body').addClass("darkedColor");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "lightDarkColor") {
		$('body').addClass("lightDarkColor");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "defaultHomeTheme") {
		$('body').addClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "primaryHomeTheme") {
		$('body').addClass("primaryHomeTheme");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "secondaryHomeTheme") {
		$('body').addClass("secondaryHomeTheme");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "basicHomeTheme") {
		$('body').addClass("basicHomeTheme");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
	}
	else if (getTargetThemeName == "moreHomeThemeOne") {
		$('body').addClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeTwo") {
		$('body').addClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeThree") {
		$('body').addClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeFour") {
		$('body').addClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeFive") {
		$('body').addClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeSix") {
		$('body').addClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeSeveen") {
		$('body').addClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeEight") {
		$('body').addClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeNine") {
		$('body').addClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeTen") {
		$('body').addClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeEleven") {
		$('body').addClass("moreHomeThemeEleven");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeTwelve");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
	else if (getTargetThemeName == "moreHomeThemeTwelve") {
		$('body').addClass("moreHomeThemeTwelve");
		$('body').removeClass("moreHomeThemeOne");
		$('body').removeClass("moreHomeThemeTwo");
		$('body').removeClass("moreHomeThemeThree");
		$('body').removeClass("moreHomeThemeFour");
		$('body').removeClass("moreHomeThemeFive");
		$('body').removeClass("moreHomeThemeSix");
		$('body').removeClass("moreHomeThemeSeveen");
		$('body').removeClass("moreHomeThemeEight");
		$('body').removeClass("moreHomeThemeNine");
		$('body').removeClass("moreHomeThemeTen");
		$('body').removeClass("moreHomeThemeEleven");
		$('body').removeClass("defaultHomeTheme");
		$('body').removeClass("primaryHomeTheme");
		$('body').removeClass("secondaryHomeTheme");
		$('body').removeClass("basicHomeTheme");
		$('body').removeClass("defaultColor");
		$('body').removeClass("primaryColor");
		$('body').removeClass("secondaryColor");
		$('body').removeClass("basicColor");
		$('body').removeClass("darkedColor");
		$('body').removeClass("lightDarkColor");
	}
}*/
$(document).ready(function() {
	$(".moreThemesShowDiv").click(function() {
		$(".moreThemes").show();
		$(".moreThemesShowDiv").hide();
		$(".moreThemesHideDiv").show();
	});
	$(".moreThemesHideDiv").click(function() {
		$(".moreThemes").hide();
		$(".moreThemesHideDiv").hide();
		$(".moreThemesShowDiv").show();
	});
});

/*function openLimitsPopOver(event) {
	$('#dxpLimitsPopOver').remove();
	$('#dxpLimitsPopOverDivParentId').remove();
	$('body').append("<div id='dxpLimitsPopOver'></div>");
	var limitspopOverDiv = `<div>
						<div class="planAndExpiryDataDivClass"  style="display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #ddd;font-weight:500;padding-bottom:8px;"><p>Plan : ${$("#dxpPopOverSubCriptionType").val()}</p><p>Expiry Date :${$("#dxpPopOverExpiryData").val().substring(0, 10)}</p></div>
						<div class="dxpLimitsPopOverDivParentClass" id='dxpLimitsPopOverDivParentId'>
									<div>
									<p>Charts</p>
									<div class="limitsPopDivClass">

										<div class="progressLimitClass">
											   <p class="number">
												<span class="num">${$("#dxpActualChartCount").val()}</span>
												<span class="divider">/</span>
											</p>
											<p class="title">${$("#dxpPopOverChartCount").val()}</p>
										</div>
										<span class="dots"></span>
										<svg class="svg">
											<circle class="circle" cx="90" cy="90" r="80" />
										</svg>
									</div>
									</div>
									<div>
									<p>DashBoards</p>
									<div class="limitsPopDivClass">

										<div class="progressLimitClass">

											<p class="number">
												<span class="num">${$("#dxpActualDashBoardsCount").val()}</span>
												<span class="divider">/</span>
											</p>
											<p class="title">${$("#dxpPopOverDashBoardCount").val()}</p>
										</div>
										<span class="dots"></span>
										<svg class="svg">
											<circle class="circle" cx="90" cy="90" r="80" />
										</svg>
									</div>
									</div>
									<div>
									<p>Space</p>
									<div class="limitsPopDivClass">

										<div class="progressLimitClass">

											<p class="number">
												<span class="num">${$("#dxpActualSpaceCount").val()}</span>
												<span class="divider">/</span>
											</p>
											<p class="title">${$("#dxpPopOverSpaceCnt").val()}<span>GB</span></p>
										</div>
										<span class="dots"></span>
										<svg class="svg">
											<circle class="circle" cx="90" cy="90" r="80" />
										</svg>
									</div>
								</div>
								</div>
								</div>`;
	$('#dxpLimitsPopOver').html(limitspopOverDiv);
	var clickX = event.pageX;
	var clickY = event.pageY;
	var offset = { left: 792, top: 37 };
	var position = "bottom";
	$("#dxpLimitsPopOver").jqxPopover({
		//offset: offset,
		position: "bottom",
		width: 350,
		//                        height: 150,
		autoClose: true,
		title: "Subscription Details",
		showCloseButton: true,
		selector: $('.limitsIcon'),

	});
	$("#dxpLimitsPopOver").jqxPopover("open");
	var limitItem = $('.limitsPopDivClass').toArray();
	limitItem.forEach(item => {
		let numElement = item.querySelector('.num');
		let endNumber = parseInt(item.querySelector('.title').innerText);
		let num = parseInt(numElement.innerText);
		let count = 0;
		let time = 2000 / num;
		let circle = item.querySelector('.circle');
		setInterval(() => {
			if (count == num) {
				clearInterval();
			} else {
				count += 1;
				numElement.innerText = count;
			}
		}, time)
		circle.style.strokeDashoffset
			= 503 - (503 * (num / endNumber));
		let dots = item.querySelector('.dots');
		dots.style.transform =
			`rotate(${360 * (num / endNumber)}deg)`;
		if (num == endNumber) {
			dots.style.opacity = 0;
		}
	})



}*/



function setuserProfileUpdateIcon(input) {
	if (input.files && input.files[0]) {
		if (input.files[0].size <= 5000000) {
			var reader = new FileReader();
			reader.onload = function(e) {
				showLoader();
				$.ajax({
					type: 'POST',
					dataType: 'JSON',
					url: 'setuserProfileUpdateIcon',
					traditional: true,
					cache: false,
					async: true,
					data: {
						imgURL: e.target.result,
						//                        userName: $('#userProfileImgDiv').attr('objstr'),
						fileName: input.files[0].name
					},
					//                    data: {
					//                        imgURL: e.target.result,
					//                        userName: $('#userProfileImgDiv').attr('objstr'),
					//                        fileName: input.files[0].name
					//                    },
					success: function(data) {
						stopLoader();
						if (data['flag'] === true) {
							var message = data['message'] || 'Operation was successful.';
							$("#dialog").html(message);
							$("#dialog").dialog({
								resizable: false,
								title: labelObject['Message'] || 'Message',
								modal: true,
								height: 'auto',
								minHeight: 'auto',
								minWidth: 300,
								maxWidth: 'auto',
								buttons: [
									{
										text: labelObject['Ok'] || 'Ok',
										click: function() {
											$(this).html("");
											try {
												$(this).dialog("close");
											} catch (e) {
												console.error('Error closing dialog:', e);
											}
										}
									}
								]
							});
							$('#imagePreview').css('background-image', 'url(' + e.target.result + ')');
							$('.userProfileIcon .userMainProfile').attr('src', e.target.result);
							localStorage.setItem('profile_imgStr', e.target.result);
							$('#imagePreview').hide();
							$('#imagePreview').fadeIn(650);
							try {
								var $el = $(input);
								$el.wrap('<form>').closest('form').get(0).reset();
								$el.unwrap();
							} catch (ex) {
								console.error('Error resetting input field:', ex);
							}

						} else if (data['flag'] === false) {
							$("#dialog").html(data['message']);
							$("#dialog").dialog({
								resizable: false,
								title: labelObject['Message'] || 'Message',
								modal: true,
								height: 'auto',
								minHeight: 'auto',
								minWidth: 300,
								maxWidth: 'auto',
								buttons: [
									{
										text: labelObject['Ok'] || 'Ok',
										click: function() {
											$(this).html("");
											try {
												$(this).dialog("destroy");
											} catch (e) {
												console.error('Error destroying dialog:', e);
											}
											try {
												$(this).dialog("close");
											} catch (e) {
												console.error('Error closing dialog:', e);
											}
										}
									}
								],
								open: function() {
									$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
									$(".visionHeaderMain").css("z-index", "999");
									$(".visionFooterMain").css("z-index", "999");
								},
								beforeClose: function() {
									$(".visionHeaderMain").css("z-index", "99999");
									$(".visionFooterMain").css("z-index", "99999");
								}
							});
							try {
								var $el = $(input);
								$el.wrap('<form>').closest('form').get(0).reset();
								$el.unwrap();
							} catch (ex) {
								console.error('Error resetting input field:', ex);
							}

						} else {
							$("#dialog").html(data['message'] || 'An unknown error occurred.');
							$("#dialog").dialog({
								resizable: false,
								title: labelObject['Message'] || 'Message',
								modal: true,
								height: 'auto',
								minHeight: 'auto',
								minWidth: 300,
								maxWidth: 'auto',
								buttons: [
									{
										text: labelObject['Ok'] || 'Ok',
										click: function() {
											$(this).html("");
											try {
												$(this).dialog("close");
											} catch (e) {
												console.error('Error closing dialog:', e);
											}
										}
									}
								]
							});
						}
					},
					error: function(jqXHR, textStatus, errorThrown) {
						stopLoader();
						try {
							var $el = $(input);
							$el.wrap('<form>').closest('form').get(0).reset();
							$el.unwrap();
						} catch (ex) {

						}
					}
				});
			}
			reader.readAsDataURL(input.files[0]);

		} else {
			try {
				var $el = $(input);
				$el.wrap('<form>').closest('form').get(0).reset();
				$el.unwrap();
			} catch (ex) {

			}
			//            $("#dialog1").html('Max size of file is 5 MB');
			//            $("#dialog1").dialog({ resizable: false,
			//                title: 'Image Size',
			//                modal: true,
			//                width: 300,
			//                height: "auto",
			//                maxHeight: 100,
			//                fluid: true,
			//                dialogClass: "dialogFactsAndStatsDiv event-Open_feedBack-Dialog"
			//            });
			closesettingPannel();
			var buttonArray = [
				{
					Ok: function() {
						closesettingPannel();
						$(this).html("");
						$(this).dialog("close");
						$(this).dialog("destroy");
					}
				}

			];
			showButtonPopupMessage("Max size of file is 5 MB", buttonArray, "Message");
		}
	}
}


function resetToDefault() {
	let userLogin = localStorage['userName'];
	$(".dxpLoginHeader").css({
		"background": "inherit",
		"background-image": "inherit"
	});
	$(".dxpLoginHeader").removeClass('lightthemeColors white-bg');
	localStorage.removeItem(`${userLogin}_headerTheme`);
	$(".dxpLoginHeader img").css("filter", "none");
	$(".ImageIconThemeShadeRight").removeClass("activeAsbgTheme");
	$(".ImageIconThemeShadeLeft").removeClass("activeAsbgTheme");
	$(".ImageIconThemeShadeRight").removeClass("activeAsbgImg");
	$(".ImageIconThemeShadeLeft").removeClass("activeAsbgImg");
	//updateUserThemes("backgroundTheme", "Default");
}

function openLimitsPopOver(event) {
	$('.popover').popover('hide');
	$('#dxpLimitsPopOverDivParentId').remove();
	var selectorID = $("#limitIconDivID");

	var limitspopOverDiv = `<div>
                                    <div class="planAndExpiryDataDivClass"  style="display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #ddd;font-weight:500;padding-bottom:8px;"><p>Plan : ${$("#dxpPopOverSubCriptionType").val()}</p><p>Expiry Date :${$("#dxpPopOverExpiryData").val().substring(0, 10)}</p></div>
                                    <div class="dxpLimitsPopOverDivParentClass" id='dxpLimitsPopOverDivParentId'>
                                                <div>
                                                <p>Charts</p>
                                                <div class="limitsPopDivClass">
            
                                                    <div class="progressLimitClass">
                                                           <p class="number">
                                                            <span class="num">${$("#dxpActualChartCount").val()}</span>
                                                            <span class="divider">/</span>
                                                        </p>
                                                        <p class="title">${$("#dxpPopOverChartCount").val()}</p>
                                                    </div>
                                                    <span class="dots"></span>
                                                    <svg class="svg">
                                                        <circle class="circle" cx="90" cy="90" r="80" />
                                                    </svg>
                                                </div>
                                                </div>
                                                <div>
                                                <p>Dashboards</p>
                                                <div class="limitsPopDivClass">
            
                                                    <div class="progressLimitClass">
            
                                                        <p class="number">
                                                            <span class="num">${$("#dxpActualDashBoardsCount").val()}</span>
                                                            <span class="divider">/</span>
                                                        </p>
                                                        <p class="title">${$("#dxpPopOverDashBoardCount").val()}</p>
                                                    </div>
                                                    <span class="dots"></span>
                                                    <svg class="svg">
                                                        <circle class="circle" cx="90" cy="90" r="80" />
                                                    </svg>
                                                </div>
                                                </div>
                                                <div>
                                                <p>Space</p>
                                                <div class="limitsPopDivClass">
            
                                                    <div class="progressLimitClass">
            
                                                        <p class="number">
                                                            <span class="num">${$("#dxpActualSpaceCount").val()}</span>
                                                            <span class="divider">/</span>
                                                        </p>
                                                        <p class="title">${$("#dxpPopOverSpaceCnt").val()}<span>GB</span></p>
                                                    </div>
                                                    <span class="dots"></span>
                                                    <svg class="svg">
                                                        <circle class="circle" cx="90" cy="90" r="80" />
                                                    </svg>
                                                </div>
                                            </div>
                                            </div>
                                            </div>`;


	var limitsTemplateDiv = `<div class="popover " role="tooltip" id=''><div class="arrow"></div><div class='popOverheader'><div class='popoverTitle'><h3 class="popover-header">Subscription Details</h3></div> <div class='closePapoverClass' onclick = closebsPopOver(event)><img src='images/close.png' /></div></div><div class="popover-body"></div></div>`;
	selectorID.popover({ animation: true, title: 'Subscription Details', trigger: 'click', content: limitspopOverDiv, template: limitsTemplateDiv, html: true, sanitize: false, boundary: 'viewport', placement: 'auto', selector: 'limitsIcon' });
	selectorID.popover('show');

	/* $('#dxpLimitsPopOver').html(limitspopOverDiv);
	 var clickX = event.pageX;
	 var clickY = event.pageY;
	  var offset = { left: 792, top: 37 };
	 var position = "bottom";
	 $("#dxpLimitsPopOver").jqxPopover({
						 //offset: offset,
						 position: "bottom",
						 width: 350,
 //                        height: 150,
						 autoClose: true,
						 title: "Subscription Details",
						 showCloseButton: true,
						 selector: $('.limitsIcon'),
 
					 });
					  $("#dxpLimitsPopOver").jqxPopover("open");*/
	var limitItem = $('.limitsPopDivClass').toArray();
	limitItem.forEach(item => {
		let numElement = item.querySelector('.num');
		let endNumber = parseInt(item.querySelector('.title').innerText);
		let num = parseInt(numElement.innerText);
		let count = 0;
		let time = 2000 / num;
		let circle = item.querySelector('.circle');
		setInterval(() => {
			if (count == num) {
				clearInterval();
			} else {
				count += 1;
				numElement.innerText = count;
			}
		}, time)
		circle.style.strokeDashoffset
			= 503 - (503 * (num / endNumber));
		let dots = item.querySelector('.dots');
		dots.style.transform =
			`rotate(${360 * (num / endNumber)}deg)`;
		if (num == endNumber) {
			dots.style.opacity = 0;
		}
	})



}

function showAnalyticsHelp(type) {
	//showLoader();
	var link = "";
	var title = "";
	var frameContent = "";
	var templateId = "";
	if (type === "video") {
		link = "https://www.youtube.com/embed/QeAJRZrFPUc";
		title = labelObject['Help Video'] || 'Help Video'
		frameContent = `<iframe src="${link}" id="contentFrame" style="width:100%; height:100%;" frameborder="0"></iframe>`;

	}
	else {
		link = "https://example.com/sample-document.pdf";
		title = labelObject['Help Document'] || 'Help Document'
		templateId = "HELP_DOCUMENT";
		frameContent = "<iframe id='contentFrame' frameborder='0' height='100%' width='100%' src='getDataBasedOnTemplateId?templateId=" + templateId + "'/>";
	}

	$("#dialog").html(frameContent);
	$("#dialog").dialog({
		resizable: false,
		title: title,
		modal: true,
		width: 950,
		height: 500,
		fluid: true,
		buttons: [
			{
				text: labelObject['Ok'] || 'Ok',
				click: function() {
					$(this).html("");
					try {
						$(this).dialog("close");
					} catch (e) {
						console.error('Error closing dialog:', e);
					}
				}
			}
		],
		open: function() {
			$(this).closest(".ui-dialog").css({ "z-index": 9999 });
			$(".visionHeaderMain, .visionFooterMain").css("z-index", 9999);




		},
		beforeClose: function(event, ui) {
			$(this).html("");
			$(this).html("destroy");
			$(".visionHeaderMain").css("z-index", "9999");
			$(".visionFooterMain").css("z-index", "9999");
		}
	});


}