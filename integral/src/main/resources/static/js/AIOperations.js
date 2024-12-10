/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.*/
var aiResultBoxCount = 0;
var onWhichPageId = 'DASHBOARD';
var continueTyping = true;
var activeDomainMenu = "";
var isAILensOnOff = true;
var micStartStopFlag = false;
$(function() {
	$("#AILensSwitchId").change(function() {
		if (($("#AILensSwitchId").prop("checked"))) {
			openAINavigation();
		} else {
			closeAINavigation();
		}
	});
});
function openAINavigation() {

	//if ($("#AILensSwitchId").prop('checked') == true) {
	if (isAILensOnOff) {
		$('#introGuiderAiId').show();
		$('.visionVisualizeChartBoxClass').show();
		document.getElementById("myNav").style.width = "400px";
		$("#myNav").addClass('pannelAIwrapper');
		$(".dragLeftArrowDiv").show();
		$(".dragRightArrowDiv").hide();
		loadInitialButtons('LENS_HEADER_ICONS');
		//loadInitialQuestions(onWhichPageId,'','aiLensHomeDivId');
		toggleMic('aiTypedValue');
		$(".aiLensImgSrcAppend").attr('src', 'images/IG_CO-Pilot.png');
		loadLanguageAndKeyboard();
		var buttonContainer = $('.aiTabsContainer .aiPrevCopilotCls button.active');
		var clearTabIdFromButtonAttr = buttonContainer.attr('clear-id');
		if (!clearTabIdFromButtonAttr.includes(',') && $('#' + clearTabIdFromButtonAttr).children().length == 0) {
			if ($('#aiLensHomeDivId').children().length == 0) {
				loadInitialQuestions(onWhichPageId, '', 'aiLensHomeDivId');
			}
			if (clearTabIdFromButtonAttr == 'aiLensMainDataAnalyticsDivId') {
				startIntegralALLensHomeModules('DataAnalytics', clearTabIdFromButtonAttr);
			}
			if (clearTabIdFromButtonAttr == 'aiLensDataIntegrationDivId') {
				startIntegralALLensHomeModules('DataIntegration', clearTabIdFromButtonAttr);
			}
			if (clearTabIdFromButtonAttr == 'aiLensDataMigrationDivId') {
				startIntegralALLensHomeModules('DataMigration', clearTabIdFromButtonAttr);
			}
			if (clearTabIdFromButtonAttr == 'aiLensQuickInsightsDivId') {
				filterSelection('Analytics', clearTabIdFromButtonAttr);
			}
		}
		else {
			if ($('#aiLensHomeDivId').children().length == 0) {
				loadInitialQuestions(onWhichPageId, '', 'aiLensHomeDivId');
			} else {
				if (clearTabIdFromButtonAttr == 'aiLensQuickAnalyticsContentDivId,aiLensQuickAnalyticsQuestionsDivId') {
					startIntegralALLensAnalytics('QuickAnalytics', "aiLensQuickAnalyticsDivId");
				}
			}
		}

	} else {
		closeAINavigation();
	}


}
function closeAINavigation() {
	isAILensOnOff = false;
	$("#AILensSwitchId").prop('checked', function(_, checked) {
		return false;
	});
	$('#myBtnContainer').empty();
	$('.intelliSenseChartVisualizeDivClass').empty();
	$(".aiLensImgSrcAppend").attr('src', '');
	$("#myNav").removeClass('pannelAIwrapper');
	//    $("#myNav").resizable('destroy');
	$("#myNav").css('left', 'inherit');
	document.getElementById("myNav").style.width = "0%";
	//    $(".aiChatContainer").html('');
	$(".aiNotificationsResultClass").html('');
	$("#stopResponsingID").hide(1000);
	$("#aiTypedValue").attr('readonly', false);


}

function filterSelection(getaiTabName, showDivId) {
	activeDomainMenu = getaiTabName;
	$("#aiLensHomeDivId").hide();
	$("#aiLensConvAIDivId").hide();
	$("#aiLensQuickInsightsDivId").hide();
	$("#aiLensQuickAnalyticsDivId").hide();
	$("#" + showDivId).show();
	if (!$('.aiChatgptResponseContainer').children().length > 0) {
		var randomNumber = generateRandomNumber();
		var msgText = "<p>You have below Files/Tables, Choose one from the list.</p>";
		var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                         <div class='convai-left-message'>${msgText}</div>
                         </div>`;
		//$('.aiChatgptResponseContainer').append(msgDiv);
		getAILensFirstHeaders(showDivId);
		//getAILensInsightsExistingTables();
	}

}
function w3AddClass(element, name) {
	var i, arr1, arr2;
	arr1 = element.className.split(" ");
	arr2 = name.split(" ");
	for (i = 0; i < arr2.length; i++) {
		if (arr1.indexOf(arr2[i]) == -1) {
			element.className += " " + arr2[i];
		}
	}
}

function w3RemoveClass(element, name) {
	var i, arr1, arr2;
	arr1 = element.className.split(" ");
	arr2 = name.split(" ");
	for (i = 0; i < arr2.length; i++) {
		while (arr1.indexOf(arr2[i]) > -1) {
			arr1.splice(arr1.indexOf(arr2[i]), 1);
		}
	}
	element.className = arr1.join(" ");
}

function leftDragAIPanel() {
	$(".dragRightArrowDiv").show();
	const currentWidth = $("#myNav").width();
	const newWidth = currentWidth + 2 + 300;
	$("#myNav").animate({ width: newWidth }, 200, function() {
		// Animation complete callback
		if (newWidth >= 1200) {
			$(".dragLeftArrowDiv").hide();
			$("#dragLeftArrowImgId").off("click");
			var modalObj = {
				title: 'AI Message',
				body: "<div class='aiUserNotif'> This is the maximum we can expand. </div>"
			};
			var buttonArray = [
				{
					text: 'OK',
					click: function() {
					},
					isCloseButton: true
				}
			];
			modalObj['buttons'] = buttonArray;
			createModal("dataDxpSplitterValue", modalObj);
			$(".modal-backdrop").show();
			$(".modal-dialog").addClass("modal-md");
			$("#dataDxpSplitterValue").css("z-index", "99999");


		}
	});
}
function rightDragAIPanel() {
	try {
		$(".dragLeftArrowDiv").show();
		const currentWidth = $("#myNav").width();
		let newWidth = currentWidth + 2 - 300;

		if (newWidth < 400) {
			newWidth = 400;
		}

		$("#myNav").animate({ width: newWidth }, 200, function() {
			if (newWidth <= 400) {
				$(".dragRightArrowDiv").hide();
				$(".dragLeftArrowDiv").show();
				$("#dragRightArrowImgId").off("click");
			}
		});
	} catch (e) {
		console.error(e);
	}
}
var searchBar = true;
function showAISearch() {
	if (searchBar) {
		$('.searchAiBarDiv').delay(200).slideDown(600);
		searchBar = false;
		$('.aiSearh-input').val('');
		$('#clearAISearch').on('click', function() {
			$('.aiSearh-input').val('');
		});
	} else {
		$('.searchAiBarDiv').delay(200).slideUp(600);
		searchBar = true;
	}
}
function showAINotification() {
	showaiLoader();
	$('#stopResponsingID').show(1000);
	$("#aiTypedValue").attr('readonly', true);
	$.ajax({
		type: "POST",
		url: 'showAILensNotificationsData',
		data: {
			notificationFlag: "Y",
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopaiLoader();
			aiAutoScrollContainer();
			if (response != null && !jQuery.isEmptyObject(response) && response != "") {
				var msgList = JSON.parse(response);

				Object.entries(msgList).forEach(([key, value]) => {
					showMsgAndReplyInAILens("", value);
				});

			}
			stopaiLoader();
		}

	});
}
function showAITypeSearchResults(searchWord) {
	showaiLoader();
	$('#stopResponsingID').show(1000);
	aiAutoScrollContainer();
	if (searchWord == null || searchWord == '' || searchWord == undefined) {
		searchWord = $("#aiTypedValue").val();
	}
	$("#aiTypedValue").attr('readonly', true);
	var url = "showAITypedValueResults";
	if (searchWord != null && searchWord != undefined && searchWord != "") {
		$.ajax({
			type: "POST",
			url: url,
			data: {
				aiTypedValue: searchWord,
			},
			traditional: true,
			cache: false,
			success: function(response) {
				stopaiLoader();
				aiAutoScrollContainer();
				const notificationJson = [];
				$("#aiTypedValue").val('');
				if (response != null && response != undefined && response != "") {
					var responseObj = JSON.parse(response);
					$(".typed-cursor").hide();
					var length = Object.keys(responseObj).length;
					for (var i = 0; i < length; i++) {
						var dataObj = {};
						var messagePopupCreation = "<div class='aiLensResultDataClass'>" + responseObj[(i + 1) + 'row'] + "</div>";

						dataObj['id'] = i + 1;
						dataObj['notif'] = messagePopupCreation;
						notificationJson.push(dataObj);

					}
					const notificationStrings = notificationJson.map((elem) => elem.notif);
					$(".typed-cursor").hide();
					animateListItem(0);
					function animateListItem(index) {
						if (index < notificationJson.length) {
							const listItem = $(`<div class='airesponseWrapperDiv' data-id='${notificationJson[index].id}'><div class='aiLensMainResultBoxClass' id='aiLensMainResultBoxClass'>`
								+ `<div class='listItemsText'></div>`
								+ `<div class='aiLensImageDataClass' id='aiLensImageDataClass'>`
								+ `<span id='aiResulBoxLikeClassId` + aiResultBoxCount + `'><img src=\"images/like_blue.png\" title='Like' style='width:20px;curser:pointer;'></span>`
								+ `<span id='aiResulBoxDisLikeClassId` + aiResultBoxCount + `'><img src=\"images/dislike_blue.png\" title='Dislike' style='width:20px;curser:pointer;'></span>`
								+ `<span id='aiResulBoxCopyClassId` + aiResultBoxCount + `'><img src=\"images/aiCopy.png\" title='Copy' style='width:20px;curser:pointer;'></span>`
								+ `<span id='aiResulBoxDownloadClassId` + aiResultBoxCount + `'><img src=\"images/aiDownload.png\" title='Download' style='width:20px;curser:pointer;' id='aiTextDwnldBtn` + aiResultBoxCount + `' onclick="downloadAIGivenData()"></span>`
								+ `</div>`
								+ `</div>`);
							$(".aiChatgptResponseContainer").append(listItem);
							//                            $(".typed-cursor").hide();
							const typed = new Typed(listItem.find('.listItemsText')[0], {
								strings: [notificationStrings[index]],
								typeSpeed: 50,
								onComplete: function() {
									console.log('Animation completed for', notificationJson[index].id);
									aiAutoScrollContainer();
									$(".typed-cursor").hide();
									$("#stopResponsingID").hide();
									$("#aiTypedValue").attr('readonly', false);
									aiResultBoxCount++;
									animateListItem(index + 1);
									//                                    if (index < notificationJson.length) {
									//                                        var dxpAdavanceSearchOptions = $('#dxpAdavanceSearchOptions').val();
									//                                        if (dxpAdavanceSearchOptions != null && dxpAdavanceSearchOptions != undefined
									//                                                && dxpAdavanceSearchOptions != '' && dxpAdavanceSearchOptions == 'D') {
									//                                            var userval = $('#SearchResult').val();
									//                                            var searchKeyword = "";
									//                                            var aiSearchType = '';
									//                                            if (aiSearchType != null && aiSearchType != undefined && aiSearchType != "" && aiSearchType != "Uom") {
									//                                                searchKeyword = userval + ' ' + searchWord + ' ' + 'Values';
									//                                                aiSearchType = "Uom";
									//                                            } else {
									//                                                searchKeyword = userval + ' ' + searchWord + ' ' + 'Uoms';
									//                                                aiSearchType = "";
									//                                            }
									//                                            showAITypeSearchResults(searchKeyword);
									//                                        }
									//                                    }
									//                                    showRecentPromptData();
								}

							});
							$('#stopResponsingID').click(function() {
								typed.stop();
								$("#aiTypedValue").removeAttr("readonly");
								$('#stopResponsingID').hide();
							});

						}

					}


					stopaiLoader();
					//            $(".aiNotificationsResultClass").html(response);
				}
			}
		});
	} else {
		var modalObj = {
			title: labelObject['Message'] != null ? labelObject['Message'] : 'AI Message',
			body: "<div id='successmsg'>Please Enter a word to search.</div>"
		};
		var buttonArray = [
			{
				text: 'OK',
				click: function() {
				},
				isCloseButton: false
			},
			{
				text: 'Cancel',
				click: function() {
				},
				isCloseButton: true
			}
		];
		modalObj['buttons'] = buttonArray;
		createModal("dataDxpSplitterValue", modalObj);
		$(".modal-backdrop").show();
		$(".modal-dialog").addClass("modal-md");
		stopaiLoader();
	}
}
function showAIKeyDownResults(event) {
	if (event.keyCode == 13) {
		showAILensReply();
	}
	let userInput = $("#aiTypedValue").val();
	if (userInput.trim() == "") {
		$(".userAIInputText img").css("filter", "");
	} else {
		$(".userAIInputText img").css("filter", "inherit");
	}
}

function showaiLoader() {
	$("#threeDotsLoader").css("opacity", "0.99");
	$("#threeDotsLoader").css("display", "block");
	$("html").css("pointer-events", "none");

}
function stopaiLoader() {
	$("#threeDotsLoader").css("display", "none");
	$("html").css("pointer-events", "auto");

}

function loadInitialButtons(uxIconsType) {
	$(".conversationalAIIcon").hide();
	let displayButtonsStatus = false;
	let showLessContainer = false;
	if (uxIconsType == null || uxIconsType == undefined || uxIconsType == "") {
		uxIconsType = "LENS_BODY_ICONS";
		getAILensAnalyticsConfig();
	}
	$.ajax({
		type: "POST",
		url: 'loadIntialButtonsData',
		data: {
			uxIconsType: uxIconsType,
		},
		traditional: true,
		cache: false,
		success: function(response) {
			if (response != null && response != undefined && response != "") {
				if (uxIconsType != null && uxIconsType != undefined && uxIconsType != "" && uxIconsType != "LENS_HEADER_ICONS") {
					var buttonsJson = JSON.parse(response);
					$("#myBtnContainer").off("click", "#showmoreContent");
					$("#myBtnContainer").off("click", "#showLessContent");
					let buttonsToShowInitially = 5; // taking a number to display initial values or say elements
					let hiddenButtons = buttonsJson.slice(buttonsToShowInitially); // we loop thorugh the the value we provided to the end of array
					displayButtons(buttonsJson.slice(0, buttonsToShowInitially)); // we load the initial values when the dom loads
					//                    $("#myBtnContainer").on("click", "#showmoreContent", function () {
					//                        if (!displayButtonsStatus) {
					//                            displayButtons(hiddenButtons, !showLessContainer);
					//                            displayButtonsStatus = true;
					//                            $(this).hide();
					//                            $("#showLessContent").show();
					//                        }
					//                    });
					//                    $("#myBtnContainer").on("click", "#showLessContent", function () {
					//                        $("#myBtnContainer").empty();
					//                        displayButtons(buttonsJson.slice(0, buttonsToShowInitially));
					//                        displayButtonsStatus = false;
					//                        showLessContainer = false;
					//                        $("<button class='btn' id='showmoreContent'><span class='aitabimage'><img src='images/ai-show_plus.png' width='18px' /></span> <span class='aitabTitle'>Show More</span></button>").appendTo("#myBtnContainer");
					//                    });
					//                    $("<button class='btn' id='showmoreContent'><span class='aitabimage'><img src='images/ai-show_plus.png' width='18px' /></span> <span class='aitabTitle'>Show More</span></button>").appendTo("#myBtnContainer");

				} else {
					var buttonsJson = JSON.parse(response);
					$(".aipanelRightIconsDiv").html(buttonsJson);
				}
			}
		}
	});
}

function displayButtons(dataArray, showLessContainer) {
	let container = $("#myBtnContainer");
	dataArray.forEach(data => {
		container.append(`<button class="btn animation-anime" onclick="showMsgAndReplyInAILens('${data.text}','${data.reply}')"> <span class='aitabimage'><img src='${data.img}' width='18px' /></span><span class='aitabTitle'>${data.text}</span></button>`);
	});

	if (showLessContainer) {
		$("<button class='btn' id='showLessContent'> <span class='aitabimage'><img src='images/aiShowMore.png' width='18px' /></span> <span class='aitabTitle'>Show less</span></button>").appendTo(container);
	}
}

function aiButtonsAnimations() {
	const animationEleme = document.querySelectorAll('.animation-anime')
	anime({
		targets: animationEleme,
		translateY: ['-300px', '0'],
		easing: 'easeInOutQuad',
		delay: function(el, i, l) {
			return i * 100;
		},
	});
}
document.addEventListener('DOMContentLoaded', () => {
	//    loadInitialButtons();
});
function aiAutoScrollContainer() {
	let container = $(".aicontentArea");
	container.animate({
		scrollTop: container[0].scrollHeight
	}, 100, "swing");
	container = $('.aiLensAnalyticsDivClass');
	if ($('.aiLensAnalyticsDivClass')[0]) {
		container.animate({
			scrollTop: container[0].scrollHeight
		}, 100, "swing");
	}
	container = $('.aiLensMainDataAnalyticsDivClass');
	container.animate({
		scrollTop: container[0].scrollHeight
	}, 100, "swing");
	container = $('.aiLensHomeDivClass');
	container.animate({
		scrollTop: container[0].scrollHeight
	}, 100, "swing");
	container = $('.aiLensConvAIDivClass');
	if ($('.aiLensConvAIDivClass')[0]) {
		container.animate({
			scrollTop: container[0].scrollHeight
		}, 100, "swing");
	}
	container = $('.aiChatgptResponseContainer');
	container.animate({
		scrollTop: container[0].scrollHeight
	}, 100, "swing");
}
function showNewChatData() {
	$('.visionVisualizeChartBoxClass').remove();
	$(".visionChartsAutoSuggestionUserClass").html("");
	$("#aiTypedValue").trigger("focus");
	$('.aiSearh-input').val('');
	//   startIntegralALLens();
}
function showAIDictClickedValResults(clickedValue) {
	openAINavigation();
	var clickedPropertyVal = $("#AIClickedProperty").val();
	var searchKeyword = clickedPropertyVal + ' ' + clickedValue;
	$("#aiTypedValue").val(searchKeyword)
	showAITypeSearchResults(searchKeyword);
}
function toggleMic1(textInputId) {

	var SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
	var recognition = new SpeechRecognition();
	$("#" + textInputId).val('');
	recognition.continuous = true;
	recognition.interimResults = true;

	recognition.lang = $(".langSelectLens").val();

	// Event delegation for mute/unmute buttons
	$(".moreaiOptions").on("click", "#muteMicId", function() {
		$(this).attr("src", "images/animationMic.gif");
		$(this).attr("id", "unmuteMicId");
		console.log('Voice recognition turned off.');
		recognition.start();
	});

	$(".moreaiOptions").on("click", "#unmuteMicId", function() {
		$(this).attr("src", "images/Mike-OutLine-Icon-01.png");
		$(this).attr("id", "muteMicId");
		var transcript = $("#" + textInputId).val();
		recognition.stop();
		if (transcript != null && transcript != undefined && transcript != '') {
			showAILensReply("Y");
			$("#unmuteMicId").attr("src", "images/Mike-OutLine-Icon-01.png");
			$("#unmuteMicId").attr("id", "muteMicId");
			//console.log('Voice recognition activated. Try speaking into the microphone.');
			//recognition.stop();
			//setTimeout(function() {
			/*var modalObj = {
				title: labelObject['Message'] != null ? labelObject['Message'] : 'AI Message',
				body: "<div id='successmsg'>Are you want to Search?.</div>"
			};
			var buttonArray = [
				{
					text: 'OK',
					click: function() {
						showAILensReply("Y");
						$("#unmuteMicId").attr("src", "images/Mike-OutLine-Icon-01.png");
						$("#unmuteMicId").attr("id", "muteMicId");
						console.log('Voice recognition activated. Try speaking into the microphone.');
						recognition.stop();
						//                         isCloseButton: false

					},
					isCloseButton: true
				},
				{
					text: 'Cancel',
					click: function() {
						$("#unmuteMicId").attr("src", "images/Mike-OutLine-Icon-01.png");
						$("#unmuteMicId").attr("id", "muteMicId");
						console.log('Voice recognition activated. Try speaking into the microphone.');
						recognition.stop();
					},
					isCloseButton: true
				}
			];
			modalObj['buttons'] = buttonArray;
			createModal("dataDxpSplitterValue", modalObj);
			$(".modal-backdrop").show();
			$(".modal-dialog").addClass("modal-md");*/
			//}, 5000);
		}
		console.log('Voice recognition activated. Try speaking into the microphone.');
		recognition.stop();
	});

	recognition.onresult = function(event) {
		var current = event.resultIndex;
		console.log("speech is :::" + event.results[current][0]);
		var transcript = event.results[current][0].transcript;
		$("#" + textInputId).val(transcript);

	}

	recognition.onstart = function() {
		console.log('Voice recognition activated. Try speaking into the microphone.');
	}

	recognition.onspeechend = function() {
		console.log('Voice recognition turned off.');
		$(this).attr("src", "images/Mike-OutLine-Icon-01.png");
		$(this).attr("id", "muteMicId");
	}

	recognition.onerror = function(event) {
		if (event.error == 'no-speech') {
			console.log('No speech was detected. Try again.');
		} else if (event.error == 'audio-capture') {
			console.log('Error capturing audio. Check your microphone settings.');
		} else if (event.error == 'not-allowed') {
			console.log('Microphone access is not allowed. Please enable it in your browser settings.');
		}
	}
}
function showAIReferenceNoAndClassBasedLinks(referenceNo, className, gridId) {
	showaiLoader();
	openAINavigation();
	$("#aiTypedValue").val(className + ' ' + referenceNo);
	$(".aiChatgptResponseContainer").append("<div class='aiAppendSenderDataClass'>" + className + ' ' + referenceNo + "</div>");
	$("#aiTypedValue").attr('readonly', true);
	$('#stopResponsingID').show(1000);
	$("#aiTypedValue").attr('readonly', true);
	$.ajax({
		type: "POST",
		url: 'getReferenceLinksBasedOnRefNoAndClass',
		data: {
			referenceNo: referenceNo,
			className: className,
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopaiLoader();
			aiAutoScrollContainer();
			const notificationJson = [];
			$("#aiTypedValue").val('');
			if (response != null && response != undefined && response != "") {
				var responseObj = JSON.parse(response);
				var mappingObj = responseObj['mappingObj'];
				var duplicateStr = responseObj['duplicateStr'];
				if (duplicateStr != null && duplicateStr != undefined && duplicateStr != "") {
					delete responseObj['duplicateStr'];
				}
				var duplicateMsg = responseObj['duplicateMsg'];
				delete responseObj['mappingObj'];
				delete responseObj['duplicateMsg'];
				$(".typed-cursor").hide();
				var length = Object.keys(responseObj).length;
				for (var i = 0; i < length; i++) {
					var dataObj = {};
					var messagePopupCreation = "<div class='aiLensResultDataClass'>" + responseObj[(i + 1) + 'row'] + "</div>";

					dataObj['id'] = i + 1;
					dataObj['notif'] = messagePopupCreation;
					notificationJson.push(dataObj);

				}
				const notificationStrings = notificationJson.map((elem) => elem.notif);
				$(".typed-cursor").hide();
				animateListItem(0);
				function animateListItem(index) {
					if (index < notificationJson.length) {
						const listItem = $(`<div class='airesponseWrapperDiv' data-id='${notificationJson[index].id}'><div class='aiLensMainResultBoxClass' id='aiLensMainResultBoxClass'>`
							+ `<div class='listItemsText'></div>`
							+ `<div class='aiLensImageDataClass' id='aiLensImageDataClass'>`
							+ `<span id='aiResulBoxLikeClassId` + aiResultBoxCount + `'><img src=\"images/like_blue.png\" title='Like' style='width:20px;curser:pointer;'></span>`
							+ `<span id='aiResulBoxDisLikeClassId` + aiResultBoxCount + `'><img src=\"images/dislike_blue.png\" title='Dislike' style='width:20px;curser:pointer;'></span>`
							+ `<span id='aiResulBoxCopyClassId` + aiResultBoxCount + `'><img src=\"images/aiCopy.png\" title='Copy' style='width:20px;curser:pointer;'></span>`
							+ `<span id='aiResulBoxDownloadClassId` + aiResultBoxCount + `'><img src=\"images/aiDownload.png\" title='Download' style='width:20px;curser:pointer;'></span>`
							+ `</div>`
							+ `</div>`);
						$(".aiChatgptResponseContainer").append(listItem);
						//                            $(".typed-cursor").hide();
						const typed = new Typed(listItem.find('.listItemsText')[0], {
							strings: [notificationStrings[index]],
							typeSpeed: 50,
							onComplete: function() {
								console.log('Animation completed for', notificationJson[index].id);
								aiAutoScrollContainer();
								AILensOperationPopup(duplicateMsg, duplicateStr, mappingObj);
								$(".typed-cursor").hide();
								$("#stopResponsingID").hide();
								$("#aiTypedValue").attr('readonly', false);
								aiResultBoxCount++;
								if (gridId != null && gridId != undefined && gridId != '') {
									$("#" + gridId).jqxGrid('updatebounddata', 'cells');
								}
								animateListItem(index + 1);
							}


						});

						$('#stopResponsingID').click(function() {
							typed.stop();
							$("#aiTypedValue").removeAttr("readonly");
							$('#stopResponsingID').hide();
						});

					}

				}


				stopaiLoader();
				//            $(".aiNotificationsResultClass").html(response);
			}
		}
	});
}

function AILensOperationPopup(duplicateMsg, duplicateStr, mappingObj) {
	var modalObj = {
		title: labelObject['Message'] != null ? labelObject['Message'] : 'AI Message',
		body: "<div id='successmsg' class='successmsg'>" + duplicateMsg + "</div><div id='showAILensUpdatedTableClass' class='showAILensUpdatedTableClass'>" + duplicateStr + "</div>"
	};
	var buttonArray = [
		{
			text: 'OK',
			click: function() {
				var itemsString = $("#itemsstring").val();
				getAILensMappingObjData(mappingObj, itemsString);
			},
			isCloseButton: true
		},
		{
			text: 'Cancel',
			click: function() {
			},
			isCloseButton: true
		}
	];
	modalObj['buttons'] = buttonArray;
	createModal("dataDxpSplitterValue", modalObj);
	$(".modal-backdrop").show();
	$(".modal-dialog").addClass("modal-md");
	stopaiLoader();

}
function getAILensMappingObjData(mappingObj, itemsString) {
	showLoader();
	$.ajax({
		type: "POST",
		url: 'getAILensMappingObjData',
		data: {
			mappingObj: mappingObj,
			itemsString: itemsString,
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopLoader();
			if (response != null) {
				var responseObj = JSON.parse(response);
				var message = responseObj['message'];
				var flag = responseObj['flag'];
				var successResult = "";
				if (flag) {
					var resultStr = responseObj['resultStr'];
					successResult = "<div id='showAILensUpdatedTableClass' class='showAILensUpdatedTableClass'>" + resultStr + "</div>";
				}
				var modalObj = {
					title: labelObject['Message'] != null ? labelObject['Message'] : 'AI Message',
					body: "<div id='successmsg' class='successmsg'>" + message + "</div>" + successResult + ""
				};
				var buttonArray = [
					{
						text: 'OK',
						click: function() {
						},
						isCloseButton: true
					},
					{
						text: 'Cancel',
						click: function() {
						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
				$(".modal-dialog").addClass("showAILensUpdatePopUpCustomClass");
			}
		}
	});
}
//function disableEnableAiLens(element) {
//    let imgElement = $("#aiLensToggleBtnId").find("img");
//    if (imgElement.attr("src").endsWith("images/aiEnable.png")) {
//        imgElement.attr("src", "images/ai_disable.png");
//        $(".openAiButton").hide();
//        $(".OpenAisection").hide();
//        $("#AIEnableOrDisableFlag").val("N");
//    } else {
//        imgElement.attr("src", "images/aiEnable.png");
//        $(".openAiButton").show();
//        $(".OpenAisection").show();
//        $("#AIEnableOrDisableFlag").val("Y");
//    }
//}
function AILensTypingAndConfirmationMsg(typingData, className, confMsgFlag, confMsgYesFunWithParams, confMsgNoFunWithParams) {
	showaiLoader();
	openAINavigation();
	var dataObj = {};
	const notificationJson = [];
	var messagePopupCreation = "<div class='aiLensResultDataClass'>" + typingData + "</div><div class='alLensConfiramtionMessage' id='alLensConfiramtionMessage'></div>";
	if (confMsgFlag != null && confMsgFlag != undefined && confMsgFlag != "" && confMsgFlag == 'Y') {
		var confirmationMessageYesOrNo = "<div class='alLensConfSubClass'><span class='alLensConfSubSpanYesClass'>\n\
<button class='btn btn-primary' onclick=\"" + confMsgYesFunWithParams + "\">Yes</button></span><span class='alLensConfSubSpanNoClass'>\n\
<button class='btn btn-primary' onclick=\"" + confMsgNoFunWithParams + "\">No</button></span></div>";
	}

	dataObj['id'] = 1;
	dataObj['notif'] = messagePopupCreation;
	notificationJson.push(dataObj);

	const notificationStrings = notificationJson.map((elem) => elem.notif);
	$(".typed-cursor").hide();
	animateListItem(0);
	function animateListItem(index) {
		if (index < notificationJson.length) {
			const listItem = $(`<div class='airesponseWrapperDiv' data-id='${notificationJson[index].id}'><div class='aiLensMainResultBoxClass' id='aiLensMainResultBoxClass'>`
				+ `<div class='listItemsText'></div>`
				//                    + `<div class='aiLensImageDataClass' id='aiLensImageDataClass'>`
				//                    + `<span id='aiResulBoxLikeClassId` + aiResultBoxCount + `'><img src=\"images/like_blue.png\" title='Like' style='width:20px;curser:pointer;'></span>`
				//                    + `<span id='aiResulBoxDisLikeClassId` + aiResultBoxCount + `'><img src=\"images/dislike_blue.png\" title='Dislike' style='width:20px;curser:pointer;'></span>`
				//                    + `<span id='aiResulBoxCopyClassId` + aiResultBoxCount + `'><img src=\"images/aiCopy.png\" title='Copy' style='width:20px;curser:pointer;'></span>`
				//                    + `<span id='aiResulBoxDownloadClassId` + aiResultBoxCount + `'><img src=\"images/aiDownload.png\" title='Download' style='width:20px;curser:pointer;'></span>`
				//                    + `</div>`
				+ `</div>`);
			$("." + className).append(listItem);
			//                            $(".typed-cursor").hide();
			const typed = new Typed(listItem.find('.listItemsText')[0], {
				strings: [notificationStrings[index]],
				typeSpeed: 50,
				onComplete: function() {

					console.log('Animation completed for', notificationJson[index].id);
					aiAutoScrollContainer();
					$(".typed-cursor").hide();
					$("#aiTypedValue").attr('readonly', false);
					if (confMsgFlag != null && confMsgFlag != undefined && confMsgFlag != "" && confMsgFlag == 'Y') {
						$("#alLensConfiramtionMessage").html(confirmationMessageYesOrNo);
						stopaiLoader();
					}
					aiResultBoxCount++;
					animateListItem(index + 1);
				}


			});

			$('#stopResponsingID').click(function() {
				typed.stop();
				$("#aiTypedValue").removeAttr("readonly");
				$('#stopResponsingID').hide();
			});

		}

	}
}
function openIntroWithAI() {
	$("#introGuiderAi").hide();
	closeAINavigation();
	homePageGuide2();
}
function closeIntroWithAI() {
	closeAINavigation();
	$("#introGuiderAi").hide();
}
function generate360Image() {
	var div = '<div id="product" style="width: 640px; height: 480px; overflow: hidden;"> <img src="images/Gasket1.png" /> <img src="images/Gasket2.png" /> <img src="images/Gasket3.png" /> <img src="images/Gasket2.png" </div>';
	var modalObj = {
		title: labelObject['Message'] != null ? labelObject['Message'] : 'AI Message',
		body: "<div id='successmsg' class='successmsg'>" + div + "</div>",
	};
	var buttonArray = [
		{
			text: 'OK',
			click: function() {
			},
			isCloseButton: true
		},
		{
			text: 'Cancel',
			click: function() {
			},
			isCloseButton: true
		}
	];
	modalObj['buttons'] = buttonArray;
	createModal("dataDxpSplitterValue", modalObj);

	var _gaq = _gaq || [];
	_gaq.push(['_setAccount', 'UA-36251023-1']);
	_gaq.push(['_setDomainName', 'jqueryscript.net']);
	_gaq.push(['_trackPageview']);

	var ga = document.createElement('script');
	ga.type = 'text/javascript';
	ga.async = true;
	ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
	var s = document.getElementsByTagName('script')[0];
	s.parentNode.insertBefore(ga, s);
	$('#product').j360();
}
function disableEnableAiLens(element) {
	closeAINavigation();
	$(".openAiButton img").attr("src", "images/aieyeLensclick.png");
	$("#myNav").addClass("disable");
	$("#AIEnableOrDisableFlag").val("N");

}


function downloadAIGivenData() {
	function downloadDiv(filename, elementClass, mimeType) {
		let closestMainResultBox = event.target.closest('.aiLensMainResultBoxClass');
		if (closestMainResultBox) {
			let resultDataChild = closestMainResultBox.querySelector('.aiLensResultDataClass');
			if (resultDataChild) {
				let elementHtml = resultDataChild.innerHTML;
				let filename = 'downloaded_file.html';
				let mimeType = 'text/plain'; // Set a valid MIME type if needed

				let link = document.createElement('a');
				link.setAttribute('download', filename);
				link.setAttribute('href', 'data:' + mimeType + ';charset=utf-8,' + encodeURIComponent(elementHtml));
				link.click();
			}
		}
	}
	var fileName = 'aiLensResultDataClass.html';
	var elementClass = 'aiLensResultDataClass';
	downloadDiv(fileName, elementClass);
}
function showAILensSpendInvEqSorceAnalysisData(basicData) {
	openAINavigation();
	showaiLoader();
	$(".aiLensMainResultBoxClass").remove();
	$.ajax({
		type: "POST",
		url: 'showAILensSpendInvEqSorceAnalysisData',
		data: {
			itemsString: JSON.stringify(basicData),
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopaiLoader();
			if (response != null && response != undefined && response != "") {
				var dataObj = {};
				const notificationJson = [];
				dataObj['id'] = 1;
				dataObj['notif'] = response;
				notificationJson.push(dataObj);

				const notificationStrings = notificationJson.map((elem) => elem.notif);
				$(".typed-cursor").hide();
				animateListItem(0);
				function animateListItem(index) {
					if (index < notificationJson.length) {
						const listItem = $(`<div class='airesponseWrapperDiv' data-id='${notificationJson[index].id}'><div class='aiLensMainResultBoxClass' id='aiLensMainResultBoxClass'>`
							+ `<div class='listItemsText'></div>`
							//                    + `<div class='aiLensImageDataClass' id='aiLensImageDataClass'>`
							//                    + `<span id='aiResulBoxLikeClassId` + aiResultBoxCount + `'><img src=\"images/like_blue.png\" title='Like' style='width:20px;curser:pointer;'></span>`
							//                    + `<span id='aiResulBoxDisLikeClassId` + aiResultBoxCount + `'><img src=\"images/dislike_blue.png\" title='Dislike' style='width:20px;curser:pointer;'></span>`
							//                    + `<span id='aiResulBoxCopyClassId` + aiResultBoxCount + `'><img src=\"images/aiCopy.png\" title='Copy' style='width:20px;curser:pointer;'></span>`
							//                    + `<span id='aiResulBoxDownloadClassId` + aiResultBoxCount + `'><img src=\"images/aiDownload.png\" title='Download' style='width:20px;curser:pointer;'></span>`
							//                    + `</div>`
							+ `</div>`);
						$(".aiNotificationsResultClass").append(listItem);
						//                            $(".typed-cursor").hide();
						const typed = new Typed(listItem.find('.listItemsText')[0], {
							strings: [notificationStrings[index]],
							typeSpeed: 50,
							onComplete: function() {

								console.log('Animation completed for', notificationJson[index].id);
								aiAutoScrollContainer();
								$(".typed-cursor").hide();
								$("#aiTypedValue").attr('readonly', false);
								aiResultBoxCount++;
								animateListItem(index + 1);
							}


						});

						$('#stopResponsingID').click(function() {
							typed.stop();
							$("#aiTypedValue").removeAttr("readonly");
							$('#stopResponsingID').hide();
						});

					}

				}
			}
		}
	});
}
function showAnalyticsBasedOnAnalysis(analysisType) {
	showLoader();
	$.ajax({
		type: "POST",
		url: 'getAnalysisResultsBasedOnType',
		data: {
			analysisType: analysisType,
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopLoader();
			var modalObj = {
				title: labelObject['Message'] != null ? labelObject['Message'] : analysisType,
				body: "<div id='aiAnalyticsBasedOnAnalysis' class='aiAnalyticsBasedOnAnalysis'>" + response + "</div>",
			};
			var buttonArray = [
				{
					text: 'OK',
					click: function() {
					},
					isCloseButton: true
				},
				{
					text: 'Cancel',
					click: function() {
					},
					isCloseButton: true
				}
			];
			modalObj['buttons'] = buttonArray;
			createModal("dataDxpSplitterValue", modalObj);
			$(".modal-dialog").addClass("modal-md");
		}
	});
}
function viewInMap(searchWord) {
	var map;
	var placesService;
	searchWord = searchWord.replace(/_/g, ' ');

	$("#map").remove();
	$("body").append("<div id=map></div>");

	var mapElement = document.getElementById('map');
	map = new google.maps.Map(mapElement, {
		center: { lat: 40.7128, lng: -74.0060 }, // Default center (New York City)
		zoom: 12, // Default zoom level
	});

	placesService = new google.maps.places.PlacesService(map);

	// Get user's current location using HTML5 Geolocation
	if (navigator.geolocation) {
		navigator.geolocation.getCurrentPosition(function(position) {
			var myLocation = {
				lat: position.coords.latitude,
				lng: position.coords.longitude
			};

			var request = {
				query: searchWord,
				fields: ['name', 'geometry'],
			};
			showLoader();
			placesService.textSearch(request, function(results, status) {
				if (status === google.maps.places.PlacesServiceStatus.OK) {
					stopLoader();
					$("#dialog").html("");
					$("#dialog").html('<div id="mapDialog" style="height: 400px; width: 100%;"></div>');

					var mapDialogElement = document.getElementById('mapDialog');
					var mapDialog = new google.maps.Map(mapDialogElement, {
						center: results[0].geometry.location,
						zoom: 15
					});

					var bounds = new google.maps.LatLngBounds();

					// Create a blue dot for user's location on the vendor locations map
					var myLocationMarker = new google.maps.Marker({
						position: myLocation,
						map: mapDialog,
						title: 'My Location',
						icon: {
							//                            path: google.maps.SymbolPath.CIRCLE,
							//                            scale: 6,  // Adjust the size of the circle
							//                            fillColor: 'blue',
							//                            fillOpacity: 0.7,
							//                            strokeColor: 'white',
							//                            strokeWeight: 2
							url: 'images/blueLocRectangle.png', // Replace with the path to your square image file
							scaledSize: new google.maps.Size(24, 24), // Adjust the size as needed
							fillColor: 'blue',
							fillOpacity: 0.7,
							strokeColor: 'white',
							strokeWeight: 2
						}
					});

					var myLocationInfoWindow = new google.maps.InfoWindow({
						content: 'Your custom information for your location'
					});

					google.maps.event.addListener(myLocationMarker, 'click', function() {
						myLocationInfoWindow.open(mapDialog, myLocationMarker);
					});

					// Add markers for each location
					for (var i = 0; i < results.length; i++) {
						addMarker(mapDialog, results[i], bounds);
					}

					// Fit the map to include both user's location and vendor locations
					bounds.extend(myLocation);
					mapDialog.fitBounds(bounds);

					// Optionally, set a maximum zoom level
					var maxZoom = 15;
					if (mapDialog.getZoom() > maxZoom) {
						mapDialog.setZoom(maxZoom);
					}

					$("#dialog").dialog({
						title: 'Vendor Locations',
						modal: true,
						height: 'auto',
						minWidth: 300,
						width: 1000,
						fluid: true,
						buttons: [{
							text: 'Ok',
							click: function() {
								$(this).html("");
								$(this).dialog("destroy");
							}
						}],
						open: function() {
							$(this).closest(".ui-dialog").css("z-index", "9999");
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});
				} else {
					stopLoader();
					console.error('Place search was not successful for the following reason: ' + status);
					// You may want to handle errors here
				}
			});
		}, function(error) {
			stopLoader();
			console.error('Error getting user location: ' + error.message);
			// Handle error getting user location
		});
	} else {
		stopLoader();
		console.error('Geolocation is not supported by this browser.');
		// Handle browser not supporting geolocation
	}
}

function addMarker(map, locationData, bounds) {
	var location = locationData.geometry.location;
	bounds.extend(location);

	var marker = new google.maps.Marker({
		position: location,
		map: map,
		title: locationData.name
	});

	var infoWindow = new google.maps.InfoWindow({
		content: `<strong>${locationData.name}</strong><br>${locationData.formatted_address}<br>Your custom information here`
	});

	google.maps.event.addListener(marker, 'click', function() {
		infoWindow.open(map, marker);
	});

	google.maps.event.addListener(marker, 'mouseover', function() {
		infoWindow.open(map, marker);
	});

	google.maps.event.addListener(marker, 'mouseout', function() {
		infoWindow.close();
	});
}

function startIntegralALLensHomeModules(aiTabName, showDivId) {
	activeDomainMenu = aiTabName;
	$("#aiLensHomeDivId").hide();
	$("#aiLensMainDataAnalyticsDivId").hide();
	$("#aiLensQuickInsightsDivId").hide();
	$("#aiLensDataIntegrationDivId").hide();
	$("#aiLensDataMigrationDivId").hide();
	$("#" + showDivId).show();
	var subscriptionType = $("#subscriptionType").val();
	if (aiTabName == 'DataAnalytics') {
		onWhichPageId = 'DESIGN VIEW';
		$('#aiTypedValue').attr('placeholder', 'Ask questions in natural language');
		if ($("#aiLensMainDataAnalyticsDivId").children().length == 0) {
			var buttonsStr = "<div id='aiLensMainDataAnalyticsButtonsDivClass' class='convai-message'><div class='convai-left-message'>"
				+ "<span class='aitabTitle'>Select from below listed options to know more</span>"
				+ "<button class='btn animation-anime' id='aiLensDataAnalyticsButtonId'"
				+ "clear-id='aiLensDataAnalyticsDivId'"
				+ "onclick=\"loadMainAnalyticsInitialQuestions('DESIGN VIEW','','aiLensDataAnalyticsDivId')\">"
				+ "<span class='aitabimage'></span><span class='aitabTitle'>Data Analytics System Guide</span>"
				+ "</button>";
			if (subscriptionType != null && subscriptionType != '' && subscriptionType != undefined
				&& (subscriptionType != 'Professional' && subscriptionType != 'Basic')) {
				buttonsStr += "<button class='btn animation-anime' id='aiCopilotConvAIButtonId'"
					+ "clear-id='aiLensConvAIDivId'"
					+ "onclick=startIntegralALLensConvAI('ConvAI','aiLensConvAIDivId')>"
					+ "<span class='aitabimage'></span><span class='aitabTitle'>Conversational AI</span>"
					+ "</button>"
					+ "<button class='btn animation-anime' id='aiCopilotQuickAnalyticsId'"
					+ "clear-id='aiLensQuickAnalyticsContentDivId,aiLensQuickAnalyticsQuestionsDivId'"
					+ "onclick=startIntegralALLensAnalytics('QuickAnalytics','aiLensQuickAnalyticsDivId')>"
					+ "<span class='aitabimage'></span><span class='aitabTitle'>NLP Question and Answers</span>"
					+ "</button>";
			}
			buttonsStr += "</div>"
				+ "</div>"
				+ "<div class='aiLensMainDataAnalyticsDataDivClass'>"
				+ "<div class='aiLensDataAnalyticsDivClass'"
				+ "id='aiLensDataAnalyticsDivId'></div>"
				+ "<div class='aiLensAnalyticsDivClass'"
				+ "id='aiLensQuickAnalyticsDivId' style='display: none;'"
				+ "tab-id='ANALYTICS'>"
				+ "<div id='aiLensQuickAnalyticsContentDivId'></div>"
				+ "</div>"
				+ "<div class='aiLensConvAIDivClass' id='aiLensConvAIDivId'"
				+ "tab-id='CONV_AI' style='display: none;'></div>"
				+ "</div>";
			$("#aiLensMainDataAnalyticsDivId").html(buttonsStr);
		}
	} else if (aiTabName == 'DataIntegration') {
		onWhichPageId = "INTEGRATION";
		$('#aiTypedValue').attr('placeholder', 'Please ask only system guidance questions');
		if ($("#aiLensDataIntegrationDivId").children().length == 0) {
			loadInitialQuestions(onWhichPageId, '', 'aiLensDataIntegrationDivId');
		}
	} else if (aiTabName == 'DataMigration') {
		onWhichPageId = "ETL";
		$('#aiTypedValue').attr('placeholder', 'Please ask only system guidance questions');
		if ($("#aiLensDataMigrationDivId").children().length == 0) {
			loadInitialQuestions(onWhichPageId, '', 'aiLensDataMigrationDivId');
		}
	}

}

function loadMainAnalyticsInitialQuestions(domain, resultStr, appendId, questionId) {
	$("#aiLensConvAIDivId").hide();
	$("#aiLensQuickAnalyticsDivId").hide();
	$("#aiLensDataAnalyticsDivId").hide();
	$("#" + appendId).show();
	if(domain == 'DESIGN VIEW')
	{
		$('#aiTypedValue').attr('placeholder', 'Please ask only system guidance questions');
	}
	if ($("#aiLensDataAnalyticsDivId").children().length == 0) {
		loadInitialQuestions(domain, resultStr, appendId, questionId);
	}
}

function startIntegralALLensHome(aiTabName, showDivId) {
	activeDomainMenu = aiTabName;
	var userName = $("#rsUserName").val();
	userName = userName.replace("_", " ");
	var user_name = "";
	userName = userName.split(' ');
	$("#aiLensHomeDivId").hide();
	$("#aiLensMainDataAnalyticsDivId").hide();
	$("#aiLensQuickInsightsDivId").hide();
	$("#aiLensDataIntegrationDivId").hide();
	$("#aiLensDataMigrationDivId").hide();
	$("#" + showDivId).show();
	$('#aiTypedValue').attr('placeholder', 'Please ask only system guidance questions');
	for (var chr = 0; chr < userName.length; chr++) {
		user_name += userName[chr].substring(0, 1).toUpperCase() + userName[chr].substring(1, userName[chr].length).toLowerCase() + ' '
	}
}
function startIntegralALLensConvAI(aiTabName, showDivId) {
	activeDomainMenu = aiTabName;
	var divId = "visionChartsAutoSuggestionUserId";
	var userName = $("#rsUserName").val();
	userName = userName.replace("_", " ");
	var user_name = "";
	userName = userName.split(' ');
	$("#aiLensHomeDivId").hide();
	$("#aiLensConvAIDivId").hide();
	$("#aiLensQuickInsightsDivId").hide();
	$("#aiLensQuickAnalyticsDivId").hide();
	$("#aiLensDataAnalyticsDivId").hide();
	$("#aiLensDataIntegrationDivId").hide();
	$("#aiLensDataMigrationDivId").hide();
	$("#" + showDivId).show();
	$('#aiTypedValue').attr('placeholder', 'Ask questions in natural language');
	if (!$('.visionChartsAutoSuggestionUserExampleParentClass .visionChartsAutoSuggestionsClass').is(':visible') && !$('.visionChartsAutoSuggestionUserExampleParentClass .visionChartsAutoSuggestionsClass').length > 0) {

		for (var chr = 0; chr < userName.length; chr++) {
			user_name += userName[chr].substring(0, 1).toUpperCase() + userName[chr].substring(1, userName[chr].length).toLowerCase() + ' '
		}

		if (showDivId != null && showDivId != '' && showDivId != undefined && showDivId == 'aiLensConvAIDivId') {
			var div = "<div id='visionChartsAutoSuggestionUserExampleParentId' class='visionChartsAutoSuggestionUserExampleParentClass'><div id='" + divId + "' class='visionChartsAutoSuggestionUserClass'></div><div id='visionChartsAutoSuggestionExampleId' class='visionChartsAutoSuggestionExampleClass'>"
				+ '<div id="chatSection"></div><div id="searchResultText"></div>'
				+ "</div></div>";

			$("#" + showDivId).append(div);

		}
		showaiLoader();
		$.ajax({
			type: "POST",
			url: "getConversationalAIMessage",
			cache: false,
			data: {
				messageId: 1
			},
			success: function(response) {
				stopaiLoader();
				if (response != null && !jQuery.isEmptyObject(response)) {
					var mainDiv = response['mainDiv'];
					$("#visionChartsAutoSuggestionUserId").append(mainDiv);
					//$(".visionConversationalAIClass").hide();
					//getAILensFirstHeaders(showDivId)
					attachRemovalAction();
					showAnimatedBubbleSequnce();
				}
			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}
		});
	}
}
function stopTypingSimulation() {
	continueTyping = false;
}
function checkQuestionInDb(textMsg) {
	$.ajax({
		type: "POST",
		url: "fetchQuestionFromDb",
		cache: false,
		data: {
			textMsg: textMsg
		},
		success: function(response) {
			if (response != null && !jQuery.isEmptyObject(response)) {
				var resultFromDB = JSON.parse(response);
				if (resultFromDB['QUES_STATUS'] && resultFromDB['AI_ANSWER_FLAG'] == 'Y') {
					showMsgAndReplyInAILens(textMsg, resultFromDB['AI_LENS_QUES_ANS']);
				} else {
					if (resultFromDB['QUES_STATUS'] && resultFromDB['AI_ANSWER_FLAG'] == 'U') {
						showMsgAndReplyInAILens(textMsg, `<a href='${resultFromDB['AI_API_URL']}'>${resultFromDB['AI_API_URL']}</a>`);
					}
				}
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}
//function loadInitialQuestions(domain,questionId,appendFlag,msg,replyMsg){
//aiAutoScrollContainer();
//    $.ajax({
//    		type: "POST",
//    		url: "fetchQuestionsFromDb",
//    		cache: false,
//    		data: {
//    			domain: domain,
//    			questionId:questionId,
//    		},
//    		success: function(response) {
//    			if (response != null && !jQuery.isEmptyObject(response)) {
//
//                    var responseObj = JSON.parse(response);
//                    if(responseObj['QUES_STATUS']){
//                        delete responseObj['QUES_STATUS'];
//                         if(appendFlag === 'INCHAT'){
//                               var randomNumber = generateRandomNumber();
//                                      var msgText = "<div class='visionChartsAutoSuggestionsClass visionAIMsgNotToAddAtEndClass'><div id='visionConversationalAI" + randomNumber + "' class='convai-message'><div class='convai-right-message'>" + msg
//                    					+ "</div><div class='convai-left-message' id='replyContainer"+randomNumber+"'></div> </div></div>";
//                    			$("#visionChartsAutoSuggestionUserId").append(msgText);
//                             if(replyMsg!= null && replyMsg!= undefined && replyMsg!='null'){
//                                 $("#replyContainer"+randomNumber).append(`<div style="display: block;width: 100%;">${replyMsg}</div>`)
//                             }
//                         }
//                        for (const key in responseObj) {
//                        if (responseObj.hasOwnProperty(key)) {
//                            const data = responseObj[key];
//                            if(appendFlag == null || appendFlag == undefined){
//                                let container = $("#myBtnContainer");
//                                container.append(`<button class="btn animation-anime" onclick="loadInitialQuestions('${data.DOMAIN}','${data.QUESTION_ID}','INCHAT','${data.QUESTION}','${data.ANSWER}')"> <span class='aitabimage'><!--<img src='images/Data-Analytics-icon.svg' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
//                            }else{
//                                  if(appendFlag === 'INCHAT')  {
//                                    let container =$("#replyContainer"+randomNumber)
//                                container.append(`<button class="btn animation-anime" onclick="loadInitialQuestions('${data.DOMAIN}','${data.QUESTION_ID}','INCHAT','${data.QUESTION}','${data.ANSWER}')"> <span class='aitabimage'><!--<img src='images/Data-Analytics-icon.svg' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
//
//                                  }
//                            }
//                        }
//                    }
//                        console.table(responseObj);
//                        if($("#replyContainer"+randomNumber+' button').length > 3){
//                            $("#replyContainer" + randomNumber + " button:gt(2)").hide();
//                            $("#replyContainer"+randomNumber).append(`<button class="btn animation-anime" id='toggleMoreButtonId' onclick="toggleMoreButtons('${randomNumber}')"> <span class='aitabimage'><!--<img src='images/Data-Analytics-icon.svg' width='18px'/>--></span><span class='aitabTitle'>Show More</span></button>`);
//
//                        }
//
//
//
//
//                    }else{
//                        if(msg!= null && msg!= undefined && msg!='' && replyMsg!= null && replyMsg!= undefined && replyMsg!=''){
//                            showMsgAndReplyInAILens(msg,replyMsg);
//                        }
//                    }
//    			}
//    		},
//    		error: function(e) {
//    			console.log(e);
//    			sessionTimeout(e);
//    			stopLoader();
//    		}
//    	});
//}

function loadInitialQuestions(domain, resultStr, appendId, questionId) {
	var resultObj, appendFlag, msg, replyMsg, questionTypeFlag;
	if (resultStr != null && resultStr != '' && resultStr != undefined) {
		for (var entitykey in dxpUnHtmlEntities) {
			var entity = dxpUnHtmlEntities[entitykey];
			var regex = new RegExp(entitykey, 'g');
			resultStr = resultStr.replace(regex, entity);

		}
		resultObj = JSON.parse(resultStr);
		if (resultObj != null && resultObj != undefined && !jQuery.isEmptyObject(resultObj)) {
			questionId = resultObj['QUESTION_ID'];
			appendFlag = resultObj['BUTTON_POSITION'];
			msg = resultObj['QUESTION'];
			replyMsg = resultObj['ANSWER'];
			questionTypeFlag = resultObj['QUESTION_TYPE'];
		}
		if (questionTypeFlag != null && questionTypeFlag != undefined && questionTypeFlag == 'YN') {
			$('.AIlensYesNoButtonsCls').remove();
			msg = 'Yes';
			var msgText = "<div class='visionChartsAutoSuggestionsClass visionAIMsgNotToAddAtEndClass'><div id='visionConversationalAI' class='convai-message'><div class='convai-right-message'>" + msg
				+ "</div> </div></div>";
			$("#" + appendId).append(msgText);
		}
	}
	aiAutoScrollContainer();
	$.ajax({
		type: "POST",
		url: "fetchQuestionsFromDb",
		cache: false,
		data: {
			domain: domain,
			questionId: questionId,
		},
		success: function(response) {
			if (response != null && !jQuery.isEmptyObject(response)) {

				var responseObj = JSON.parse(response);
				if (responseObj['QUES_STATUS']) {
					delete responseObj['QUES_STATUS'];
					var randomNumber = generateRandomNumber();
					if (msg != null && msg != undefined && msg != '' && questionTypeFlag == 'YN') {
						var msgText = "<div class='visionChartsAutoSuggestionsClass visionAIMsgNotToAddAtEndClass'><div id='visionConversationalAI" + randomNumber + "' class='convai-message'>"
							+ "<div class='convai-left-message' id='replyContainer" + randomNumber + "'><div style='width:100%'></div></div> </div></div>";
						$("#" + appendId).append(msgText);
					}
					else {
						if (msg != null && msg != undefined && msg != '') {
							var msgText = "<div class='visionChartsAutoSuggestionsClass visionAIMsgNotToAddAtEndClass'><div id='visionConversationalAI" + randomNumber + "' class='convai-message'>"
								+ "<div class='convai-left-message' id='replyContainer" + randomNumber + "'><div style='width:100%'></div></div> </div></div>";
							/*var msgText = "<div class='visionChartsAutoSuggestionsClass visionAIMsgNotToAddAtEndClass'><div id='visionConversationalAI" + randomNumber + "' class='convai-message'><div class='convai-right-message'>" + msg
								+ "</div><div class='convai-left-message' id='replyContainer" + randomNumber + "'><div style='width:100%'></div></div> </div></div>";*/
							$("#" + appendId).append(msgText);
						} else {
							if (responseObj != null && responseObj != undefined && !jQuery.isEmptyObject(responseObj) && (!questionId || responseObj[0]["ANSWER_TYPE"] == 'FM')) {
								replyMsg = responseObj[0]["ANSWER"]
								//if(replyMsg != null && replyMsg != undefined && replyMsg.trim()!=''){
								var msgText = "<div class='visionChartsAutoSuggestionsClass visionAIMsgNotToAddAtEndClass'><div id='visionConversationalAI" + randomNumber + "' class='convai-message'>"
									+ "<div class='convai-left-message' id='replyContainer" + randomNumber + "'><div style='width:100%'></div></div> </div></div>";
								$("#" + appendId).append(msgText);
								//}
							}
						}
					}
					let container = $("#replyContainer" + randomNumber);
					if (container.length == 0) {
						var msgText = "<div class='visionChartsAutoSuggestionsClass visionAIMsgNotToAddAtEndClass'><div id='visionConversationalAI" + randomNumber + "' class='convai-message'>"
							+ "<div class='convai-left-message' id='replyContainer" + randomNumber + "'><div style='width:100%'></div></div> </div></div>";
						$("#" + appendId).append(msgText);
					}
					if (replyMsg != null && replyMsg != undefined && replyMsg != '') {
						simulateTyping(replyMsg, $('#replyContainer' + randomNumber + ' div')).then(function() {
							for (const key in responseObj) {
								if (responseObj.hasOwnProperty(key)) {
									const data = responseObj[key];
									var tempResultObj = JSON.stringify(data);
									for (var entitykey in dxpHtmlEntities) {
										var entity = dxpHtmlEntities[entitykey];
										var regex = new RegExp(entitykey, 'g');
										tempResultObj = tempResultObj.replace(regex, entity);

									}
									if (appendFlag == null || appendFlag == undefined) {
										if (data['BUTTON_POSITION'] == 'INTITTLE' && data['QUESTION_TYPE'] == 'T') {
											let container = $('#introGuiderAiId');
											container.empty();
											var tittle = data['ANSWER'];
											var titleArr = tittle.split(';;');
											titleArr.forEach((t, index) => {
												container.append(`<div id="aiLensTittle${index + 1}"></div>`); // Appending title to a div with id "div1", "div2", etc.
											});
											titleArr.forEach((t, index) => {
												container = $(`#introGuiderAiId #aiLensTittle${index + 1}`);
												var ssUsername = $('#ssUsername').val();
												if (t.includes('${ssUsername}'))
													t = t.replace('${ssUsername}', ssUsername);
												simulateTyping(t, container);

											});
										}
										else {
											if (data['QUESTION_TYPE'] == 'YN') {
												container.append(`<button class="btn animation-anime" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')"> <span class='aitabimage'><!--<img src='${data.QUES_ICON}' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
												var msgText = `<div class ='convai-messageAIlensYesNoButtonsCls'><div class="convai_right_main_message"><button class="visionConversationalAIClass convai-left-message-button" onclick="showConversationAIMessage('Yes',2,1)">Yes</button><button class="visionConversationalAIClass convai-left-message-button" onclick="showConversationAIMessage('No',3,1)">No</button></div></div>`;
												container.append(msgText);
												//
											} else {
												let container = $("#" + appendId + " #replyContainer" + randomNumber);
												container.append(`<button class="btn animation-anime" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')"> <span class='aitabimage'><!--<img src='${data.QUES_ICON}' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
											}
										}
									} else {
										if (appendFlag === 'INCHAT') {
											let container = $("#" + appendId + " #replyContainer" + randomNumber);
											container.append(`<button class="btn animation-anime" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')"> <span class='aitabimage'><!--<img src='${data.QUES_ICON}' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
										} else {
											container.append(`<button class="btn animation-anime" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')"> <span class='aitabimage'><!--<img src='${data.QUES_ICON}' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
										}

									}
								}
								aiAutoScrollContainer();
							}
							if ($("#replyContainer" + randomNumber + ' button').length > 3) {
								$("#replyContainer" + randomNumber + " button:gt(2)").hide();
								$("#replyContainer" + randomNumber).append(`<button class="btn animation-anime" id='toggleMoreButtonId' onclick="toggleMoreButtons('${randomNumber}')"> <span class='aitabimage'><!--<img src='images/Data-Analytics-icon.svg' width='18px'/>--></span><span class='aitabTitle'>Show More</span></button>`);

							}
							if ($("#myBtnContainer button").length > 3) {
								$("#myBtnContainer button:gt(2)").hide();
								$("#myBtnContainer").append(`<button class="btn animation-anime" id='toggleMoreButtonsInModeBarId' onclick="toggleMoreButtonsInModeBar('myBtnContainer')"> <span class='aitabimage'><!--<img src='images/Data-Analytics-icon.svg' width='18px'/>--></span><span class='aitabTitle'>Show More</span></button>`);
							}
						});
					}
					else {
						for (const key in responseObj) {
							if (responseObj.hasOwnProperty(key)) {
								const data = responseObj[key];
								var tempResultObj = JSON.stringify(data);
								for (var entitykey in dxpHtmlEntities) {
									var entity = dxpHtmlEntities[entitykey];
									var regex = new RegExp(entitykey, 'g');
									tempResultObj = tempResultObj.replace(regex, entity);
								}
								if (appendFlag == null || appendFlag == undefined) {
									if (data['BUTTON_POSITION'] == 'INTITTLE' && data['QUESTION_TYPE'] == 'T') {
										let container = $('#introGuiderAiId');
										container.empty();
										var tittle = data['ANSWER'];
										var titleArr = tittle.split(';;');

										titleArr.forEach((t, index) => {
											container.append(`<div id="aiLensTittle${index + 1}"></div>`); // Appending title to a div with id "div1", "div2", etc.
										});
										titleArr.forEach((t, index) => {
											container = $(`#introGuiderAiId #aiLensTittle${index + 1}`);
											var ssUsername = $('#ssUsername').val();
											if (t.includes('${ssUsername}'))
												t = t.replace('${ssUsername}', ssUsername);
											simulateTyping(t, container);

										});
									}
									else {
										let container = $("#" + appendId);
										if (data['QUESTION_TYPE'] == 'YN') {
											container.append(`<button class="btn animation-anime" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')"> <span class='aitabimage'><!--<img src='${data.QUES_ICON}' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
											var msgText = `<div class ='convai-message AIlensYesNoButtonsCls'><div class="convai_right_main_message"><button class="visionConversationalAIClass convai-left-message-button" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')">Yes</button><button class="visionConversationalAIClass convai-left-message-button" ">No</button></div></div>`;
											container.append(msgText);
											//
										} else {
											let container = $("#" + appendId + " #replyContainer" + randomNumber);
											container.append(`<button class="btn animation-anime" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')"> <span class='aitabimage'><!--<img src='${data.QUES_ICON}' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
										}
									}
								} else {
									if (appendFlag === 'INCHAT') {
										//let container = $("#" + appendId + " #replyContainer" + randomNumber);
										container.append(`<button class="btn animation-anime" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')"> <span class='aitabimage'><!--<img src='${data.QUES_ICON}' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
									} else {
										container.append(`<button class="btn animation-anime" onclick="${data.QUES_ONCLICK}('${data.DOMAIN}','${tempResultObj}','${appendId}')"> <span class='aitabimage'><!--<img src='${data.QUES_ICON}' width='18px' />--></span><span class='aitabTitle'>${data.QUESTION}</span></button>`);
									}

								}
							}
							aiAutoScrollContainer();
						}
						if ($("#replyContainer" + randomNumber + ' button').length > 3) {
							$("#replyContainer" + randomNumber + " button:gt(2)").hide();
							$("#replyContainer" + randomNumber).append(`<button class="btn animation-anime" id='toggleMoreButtonId' onclick="toggleMoreButtons('${randomNumber}')"> <span class='aitabimage'><!--<img src='images/Data-Analytics-icon.svg' width='18px'/>--></span><span class='aitabTitle'>Show More</span></button>`);

						}
						if ($("#myBtnContainer button").length > 3) {
							$("#myBtnContainer button:gt(2)").hide();
							$("#myBtnContainer").append(`<button class="btn animation-anime" id='toggleMoreButtonsInModeBarId' onclick="toggleMoreButtonsInModeBar('myBtnContainer')"> <span class='aitabimage'><!--<img src='images/Data-Analytics-icon.svg' width='18px'/>--></span><span class='aitabTitle'>Show More</span></button>`);
						}
					}
					//console.table(responseObj);
				} else {
					//					if (msg != null && msg != undefined && msg != '' && replyMsg != null && replyMsg != undefined && replyMsg != '') {
					showMsgAndReplyInAILens(msg, replyMsg, appendId);
					//					}
				}
			}
			$('#aiPrevCopilotId .btn').click(function() {
				$('#aiPrevCopilotId .btn').removeClass('active');
				$(this).addClass('active');
			});
			aiAutoScrollContainer();
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}





function toggleMoreButtons(randomNumber) {
	$("#replyContainer" + randomNumber + " button:not(#toggleMoreButtonId):gt(2)").toggle();
	let showMoreButton = $("#replyContainer" + randomNumber + " #toggleMoreButtonId .aitabTitle");
	showMoreButton.text(function() {
		return $(this).text() === "Show More" ? "Show Less" : "Show More";
	});
	//console.log('moice');


}

function showAILensVideo(domain, resultStr) {
	var resultObj, questionId, appendFlag, msg, replyMsg;
	var videoLink;
	if (resultStr != null && resultStr != '' && resultStr != undefined) {
		for (var entitykey in dxpUnHtmlEntities) {
			var entity = dxpUnHtmlEntities[entitykey];
			var regex = new RegExp(entitykey, 'g');
			resultStr = resultStr.replace(regex, entity);

		}
		resultObj = JSON.parse(resultStr);
		if (resultObj != null && resultObj != undefined && !jQuery.isEmptyObject(resultObj)) {
			//            questionId = resultObj['QUESTION_ID'];
			//            appendFlag = resultObj['BUTTON_POSITION'];
			//            msg = resultObj['QUESTION'];
			//            replyMsg = resultObj['ANSWER'];
			videoLink = resultObj['VIDEO_URL'];
		}
		//        window.location.href =videoLink;
		setPopupServiceVideo('Video', videoLink);
		//window.open(videoLink, '_blank');
		//        $('#aILensVideoDialog').remove();
		//        $('body').append(`<div id="aILensVideoDialog"></div>`);
		//        $('#aILensVideoDialog').append(`<div>
		//                                               <iframe id="aiLensvideoFrame" width="560" height="315" src="" frameborder="0" allowfullscreen></iframe>
		//                                           </div>`);
		//         $("#aILensVideoDialog").dialog({
		//        autoOpen: false,
		//        modal: true,
		//        width: 600,
		//        height: 400,
		//        open: function() {
		//            $('#aiLensvideoFrame').attr('src', videoLink);
		//
		//        },
		//        close: function() {
		//
		//            $('#aiLensvideoFrame').attr('src', '');
		//            $('html').css('overflow', 'auto');
		//        }
		//    });
		//          $("#aILensVideoDialog").dialog("open");
		//
	}

}

function toggleMoreButtonsInModeBar(id) {

	$("#" + id + " button:not(#toggleMoreButtonsInModeBarId):gt(2)").toggle();
	let showMoreButton = $("#" + id + " #toggleMoreButtonsInModeBarId .aitabTitle");
	showMoreButton.text(function() {
		return $(this).text() === "Show More" ? "Show Less" : "Show More";
	});
}

function showIntroFromAILens(domain, resultStr) {
	loadPagebasedonMiraiNavigation(domain);
	var timeOut = 2000;
	if(domain == 'DASHBOARD')
	{
		timeOut = 5000;
	}
	setTimeout(function() {
		stopLoader();
		var resultObj, questionId, appendFlag, msg, replyMsg;
		if (resultStr != null && resultStr != '' && resultStr != undefined) {
			for (var entitykey in dxpUnHtmlEntities) {
				var entity = dxpUnHtmlEntities[entitykey];
				var regex = new RegExp(entitykey, 'g');
				resultStr = resultStr.replace(regex, entity);

			}
			resultObj = JSON.parse(resultStr);
			var introStepsStr = resultObj['INTRO_STEPS'];
			introStepsStr = introStepsStr = introStepsStr.replace(/'/g, '"');
			var introStepsObj;
			const insteps = [];
			var functionsToBeopened = {};
			if (introStepsStr != null && introStepsStr != undefined && introStepsStr != '') {
				introStepsObj = JSON.parse(introStepsStr);

				for (const key in introStepsObj) {
					if (Object.hasOwnProperty.call(introStepsObj, key)) {
						const stepData = introStepsObj[key];
						const step = {
							title: `<img src="${stepData.title_img}" width="150px"/><span class="toggleIconsDA">${stepData.title_desc}</span>`,
							element: stepData.element,
							intro: stepData.intro
						};
						insteps.push(step);
						var openFunction = stepData.open_function;
						if (openFunction != null && openFunction != undefined && openFunction != '') {
							functionsToBeopened[stepData.element] = stepData.open_function;
						}
					}
				}
				if (insteps.length > 0) {
					const intro3 = introJs();
					intro3.setOptions({
						nextLabel: 'Next',
						prevLabel: 'Back',
						tooltipClass: 'customTooltip'
					});

					insteps.forEach((step, index) => {
						intro3.addStep({
							title: step.title,
							element: step.element,
							intro: step.intro,

						});
					});
					if (functionsToBeopened != null && functionsToBeopened != undefined && !jQuery.isEmptyObject(functionsToBeopened)) {
						for (var functionName in functionsToBeopened) {
							var funcName = functionsToBeopened[functionName];
							var func = window[funcName];
							if (typeof func === 'function' && !$(functionName).is(':visible')) {
								func();
							}
						}
					}

					intro3.start();
				}
			}
		}
	}, timeOut);
}

function getAILensAnalyticsConfig() {
	$.ajax({
		url: 'getIntelliSenseViewModalChartConfigOptions',
		type: "POST",
		dataType: 'json',
		traditional: true,
		cache: false,
		async: false,
		data: {

		},
		success: function(response) {
			stopLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				chartFilterConfigObj = response['jsonChartFilterObj'];
			}
		}, error: function(e) {
			console.log("The Error Message is:::" + e.message);
			sessionTimeout(e);
		}
	});
}
function clearAIlensChat() {
	var buttonContainer = $('.aiTabsContainer .aiPrevCopilotCls button.active');
	var clearTabIdFromButtonAttr = buttonContainer.attr('clear-id');
	if (clearTabIdFromButtonAttr.includes(',')) {
		$(".aicontentArea .aiLensAnalyticsDivClass").css("height", "auto");
		var clearTabIdFromButtonAttrArr = clearTabIdFromButtonAttr.split(',');
		clearTabIdFromButtonAttrArr.forEach((value, index) => {
			$('#' + value).empty();
		});

	} else {
		$('#' + clearTabIdFromButtonAttr).empty();
	}

	openAINavigation();

}



function loadLanguageAndKeyboard() {
	var languages = {

		"en": {
			layout: "qwerty",
			name: "English"
		},
		"tr": {
			layout: "ms-Turkish F",
			name: "Turkish (T\u00fcrk\u00e7e)"
		},
		"ar": {
			layout: "ms-Arabic (101)",
			name: "Arabic (\u0627\u0644\u0639\u0631\u0628\u064a\u0629)"
		},
		"de": {
			layout: "ms-German",
			name: "German (Deutsch)"
		},
		"sq": {
			layout: "ms-Albanian",
			name: "ms-Albanian"
		},
		"bn": {
			layout: "ms-Bengali",
			name: "Bengali"
		},
		"te": {
			layout: "ms-Telugu",
			name: "Telugu"
		},
		"zh": {
			layout: "ms-Chinese ChaJei IME",
			name: "Chinese"
		},
		"fr": {
			layout: "ms-Belgian French",
			name: "French US"
		},
		"fr-ca": {
			layout: "ms-Canadian French",
			name: "French UK"
		},
		"nl": {
			layout: "ms-Dutch",
			name: "Dutch"
		},
		"hu": {
			layout: "ms-Hungarian 101-key",
			name: "Hungarian"
		},
		"it": {
			layout: "ms-Italian",
			name: "Italian"
		},
		"ja": {
			layout: "ms-Japanese Hiragana",
			name: "Japanese"
		},
		"ko": {
			layout: "ms-Korean",
			name: "Korean"
		},
		"no": {
			layout: "ms-Norwegian with Sami",
			name: "Norwegian"
		},
		"pt": {
			layout: "ms-Portuguese",
			name: "Portuguese PT"
		},
		"br": {
			layout: "ms-Portuguese (Brazilian ABNT)",
			name: "Portuguese BR"
		},
		"eu": {
			layout: "ms-Portuguese",
			name: "Portuguese EU"
		},
		"ro": {
			layout: "ms-Romanian (Standard)",
			name: "Romanian"
		},
		"ru": {
			layout: "ms-Russian",
			name: "Russian"
		},
		"es": {
			layout: "ms-Spanish",
			name: "Spanish ES"
		},
		"mx": {
			layout: "ms-Spanish Variation",
			name: "Spanish Mx"
		},


	};





	$('#aiTypedValue')
		.keyboard({
			openOn: null,
			stayOpen: true,
			layout: 'alpha',
			autoAccept: true,
			usePreview: false,
		})
		.addTyping();
	addLang();
	function selectLang() {
		var lang = $("#langSelectLens").val();
		var obj = languages[lang],
			kb = $('#aiTypedValue').getkeyboard();
		if (obj) {
			kb.options.language = lang;
			kb.redraw(obj.layout);
		}
	}

	$('#keyboardIdLens').unbind('click').click(function() {
		var kb = $('#aiTypedValue').getkeyboard();
		// close the keyboard if the keyboard is visible and the button is clicked a second time
		if (kb.isOpen) {
			$("#minMaxKeysIdlens").hide();
			kb.close();
		} else {
			selectLang();
			$("#minMaxKeysIdlens").show();
		}
	});

	$("#langSelectLens").change(function() {
		var kb = $('#aiTypedValue').getkeyboard();
		if (kb.isOpen) {
			selectLang();
		}
		else {
			kb.close();
		}

	});

	function addLang() {
		var html = '';
		Object.keys(languages).forEach(function(language) {
			html += `<option value="${language}">${languages[language].name}</option>`;
		});
		$('#langSelectLens')
			.html(html)
			.val($('#aiTypedValue').getkeyboard().options.language || "en")

	}
}

function startIntegralALLensAnalytics(getaiTabName, showDivId) {
	activeDomainMenu = getaiTabName;
	$("#aiLensHomeDivId").hide();
	$("#aiLensConvAIDivId").hide();
	$("#aiLensQuickInsightsDivId").hide();
	$("#aiLensQuickAnalyticsDivId").hide();
	$("#aiLensDataAnalyticsDivId").hide();
	$("#aiLensDataIntegrationDivId").hide();
	$("#aiLensDataMigrationDivId").hide();
	$("#" + showDivId).show();
	$('#aiTypedValue').attr('placeholder', 'Ask questions in natural language');
	if (!($('#aiLensQuickAnalyticsContentDivId').children().length > 0)) {
		//ajaxStart();
		//		getAILensAnalyticsQuestions();

		getAILensFirstHeaders(showDivId);
		//getAILensAnalyticsExistingTables();
	}

}

function quickInsightsAnalyticsQuestionsToggleicon() {
	/*$("#data-quickInsightsAnalyticsQuestions").toggle();*/
	$("#aiLensQuickAnalyticsDivId").toggleClass("quickAnalyticsQuestionsClass");
	/*$(".quickInsightsAnalyticsQuestionsToggleicon").css('transform', 'rotate(180deg)');*/
	if ($(".quickInsightsAnalyticsQuestionsToggleicon i").hasClass("fa-angle-double-up")) {
		$(".quickInsightsAnalyticsQuestionsToggleicon i").removeClass("fa-angle-double-up").addClass("fa-angle-double-down");
		$(".defultShowAIDiv").removeClass("quickAnalyticsQuestionsEnables");
		$(".aiLensAnalyticsDivClass .quickInsightsAnalyticsQuestionsToggleicon").css("bottom,", '50px');
		aiAutoScrollContainer();
	} else {
		$(".quickInsightsAnalyticsQuestionsToggleicon i").removeClass("fa-angle-double-down").addClass("fa-angle-double-up");
		$(".defultShowAIDiv").addClass("quickAnalyticsQuestionsEnables");
		$(".aiLensAnalyticsDivClass .quickInsightsAnalyticsQuestionsToggleicon").css("bottom,", '35%');
		aiAutoScrollContainer();
	}

}
//function getAILensFirstHeaders(showDivId){
//var tabId = $('#'+showDivId).attr('tab-Id');
//$("#" + showDivId).children().hide();
//$.ajax({
//    		type: "GET",
//    		url: 'getAILensFirstHeaders',
//    		data: {
//    				tabId: tabId,
//    				domain:onWhichPageId,
//    		},
//    		dataType: 'json',
//    		traditional: true,
//    		cache: false,
//    		success: function(response) {
//    			stopLoader();
//    			if(response != null && !jQuery.isEmptyObject(response) && response['STATUS']){
//    			var questionObj = response['questionObj'];
//    			var domain  = questionObj['DOMAIN'];
//    			loadInitialQuestions(domain, JSON.stringify(questionObj),showDivId);
//    			loadInitialQuestions()
////					$("#" + showDivId+' #aiLensFirstHeaderMsgId').remove();
////                    $("#" + showDivId).prepend(`<div id='aiLensFirstHeaderMsgId' class='aiLensFirstHeaderMsgCls'></div>`);
////                    simulateTyping(answer, $("#" + showDivId+' #aiLensFirstHeaderMsgId')).then(function() {
////                        $("#" + showDivId).children().show();
////                        if(showDivId == "aiLensConvAIDivId"){
////                            $(".visionConversationalAIClass").show();
////                        }
////                        else if(showDivId == "aiLensQuickInsightsDivId"){
////                                getAILensInsightsExistingTables();
////                        }else if(showDivId == "aiLensQuickAnalyticsDivId"){
////                                getAILensAnalyticsExistingTables();
////
////                        }
////
////                    });
//    			}
//
//
//    		},
//    		error: function (e) {
//    			stopLoader();
//    			console.error(e);
//    		}
//    	});
//}

function getAILensFirstHeaders(showDivId) {
	var tabId = $('#' + showDivId).attr('tab-Id');
	//$("#" + showDivId).children().hide();
	$.ajax({
		type: "GET",
		url: 'getAILensFirstHeaders',
		data: {
			tabId: tabId,
			domain: onWhichPageId,
		},
		dataType: 'json',
		traditional: true,
		cache: false,
		success: function(response) {
			stopLoader();
			aiAutoScrollContainer();
			if (response != null && !jQuery.isEmptyObject(response) && response['STATUS']) {

				var domain = response['DOMAIN'];
				delete response['STATUS'];
				if (showDivId === 'aiLensQuickAnalyticsDivId') {
					loadInitialQuestions(domain, JSON.stringify(response), "aiLensQuickAnalyticsContentDivId");
				} else {
					loadInitialQuestions(domain, JSON.stringify(response), showDivId);
				}

			}


		},
		error: function(e) {
			stopLoader();
			console.error(e);
		}
	});
}

function toggleOpenCloseAINavigationCheckbox() {
	isAILensOnOff = true;
	$("#AILensSwitchId").prop('checked', function(_, checked) {
		return true;
	});
}

function toggleMic(textInputId) {

	$("#" + textInputId).val('');
	if (annyang) {
		// Define commands
		const commands = {
			'*text': (text) => {
				const inputField = $("#" + textInputId);
				const currentValue = inputField.val();

				// Append the new text to the current value
				inputField.val(currentValue + ' ' + text);
			}
		};

		// Add commands to annyang
		annyang.addCommands(commands);


		$(".moreaiOptions").on("click", "#muteMicId", function() {
			$(this).attr("src", "images/animationMic.gif");
			$(this).attr("id", "unmuteMicId");
			console.log('Voice recognition turned off.');
			micStartStopFlag = true;
			annyang.start({ autoRestart: true, continuous: true });
		});

		$(".moreaiOptions").on("click", "#unmuteMicId", function() {
			$(this).attr("src", "images/Mike-OutLine-Icon-01.png");
			$(this).attr("id", "muteMicId");
			annyang.abort();
			if (micStartStopFlag == true) {
				micStartStopFlag = false;
			}
			console.log('Voice recognition activated. Try speaking into the microphone.');
			var textInput = $("#" + textInputId).val();
			if (textInput != null && textInput != "" && textInput != undefined) {
				micStartStopFlag = false;
				showAILensReply("Y");
				$("#" + textInputId).val('');
			} else {
				if (micStartStopFlag != 'sent') {
					//showAnalyticsMsg("Message", "Please speak to proceed", "errorDIvId");
				}

			}
			/*if (textInput != null && textInput != "") {
				var modalObj = {
					title: labelObject['Message'] != null ? labelObject['Message'] : 'AI Message',
					body: "<div id='successmsg'>Are you want to Search?.</div>"
				};
				var buttonArray = [
					{
						text: 'OK',
						click: function() {
							showAILensReply("Y");
							$("#unmuteMicId").attr("src", "images/Mike-OutLine-Icon-01.png");
							$("#unmuteMicId").attr("id", "muteMicId");
							console.log('Voice recognition activated. Try speaking into the microphone.');
							isCloseButton: false

						},
						isCloseButton: true
					},
					{
						text: 'Cancel',
						click: function() {
							$("#unmuteMicId").attr("src", "images/Mike-OutLine-Icon-01.png");
							$("#unmuteMicId").attr("id", "muteMicId");
							console.log('Voice recognition activated. Try speaking into the microphone.');

						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
				$(".modal-backdrop").show();
				$(".modal-dialog").addClass("modal-md");

			}*/
		});

		annyang.addCallback('end', () => {
		});
	} else {
		alert('Your browser does not support annyang.');
	}
}

function loadPagebasedonMiraiNavigation(domain) {
	showLoader();
	if (domain != null && domain != '' && domain != undefined) {
		if (domain == 'DASHBOARD') {
			if (!($(".visualizationDashboardView").length > 0)) {
				//navigationMenuUrl('homePage');
				getHomePageSelectBoxResults('CHARTS');
			}
		} else if (domain == 'DESIGN VIEW') {
			if (!($("#visualizeChartAndDataArea").length > 0)) {
				loadVisuvalization();
			}
		} else if (domain == 'INTEGRATION') {
			if (!($("#etlIntegrationNavigatePageBody").length > 0)) {
				loadIntegration('ETL');
			}

		} else if (domain == 'ETL') {
			if (!($("#etlNavigatePageBody").length > 0)) {
				loadETL('ETL');
			}
		}
	}
}