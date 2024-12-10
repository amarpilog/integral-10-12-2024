/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

var checkMsgTimeout;

$(document).ready(function() {

	// checkMsgTimeout = setInterval(checkNewMessages, 1000);
})
function loadCommunicationModule() {
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'json',
		url: 'getCommunicationModuleLayout',
		cache: false,
		data: {

		},
		success: function(response) {
			stopLoader();
			var chatTemplate = response['chatTemplate'];
			//            $("#pageBodyContent").remove();
			$("#pageBodyContent>*").hide();
			$("#chatPageBody").remove();
			$("#pageBodyContent").append('<div id ="chatPageBody" class="chat-page-body"></div>');
			//            $("#pageBody").append('<div class="page-body-content" id="pageBodyContent"></div></div>');
			$("#chatPageBody").append(chatTemplate);

			loadContacts();
			loadWebsocket();
			connect(username);
			connectWSFiles(username);
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}

function loadContacts() {
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'json',
		url: 'fetchContactsList',
		cache: false,
		data: {
			startIndex: '0',
			endIndex: '20'
		},
		success: function(response) {
			stopLoader();
			var contactsList = response['contactsList'];
			$(".contactsList").html(contactsList);


			fetchUnreadMessageCount();

			$("#cmContactsListStartIndex").val("20");
			var timeout;
			$(".contactsList").unbind("scroll").on("scroll", function(event) {
				console.log("scrolled ");
				if ($(this).scrollTop() + $(this).innerHeight() >= ($(this)[0].scrollHeight - 2)) {
					console.log("iam in scroll functionality...........");
					clearTimeout(timeout);
					timeut = setTimeout(function() {
						loadContactsListOnScroll(event);
					}, 50);
				}
			});

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}
;

function loadContactChat(contactUserName) {
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'json',
		url: 'fetchContactChatTemplate',
		cache: false,
		data: {
			contactUserName: contactUserName
		},
		success: function(response) {
			stopLoader();
			var chatContentLayout = response['chatContentLayout'];
			// var videoCallDivs = response['videoCallDivs'];
			$(".chatContent").html(chatContentLayout);

			$.ajax({
				type: "post",
				traditional: true,
				dataType: 'json',
				url: 'fetchContactChat',
				cache: false,
				data: {
					contactUserName: contactUserName
				},
				success: function(response) {
					stopLoader();
					var contactChatLog = response['contactChatLog'];
					$(".contactChatLog").html(contactChatLog);
					$('.contactChatLog').scrollTop($('.contactChatLog')[0].scrollHeight);
					$("#" + contactUserName + "_unread").text("");
					$("#" + contactUserName + "_unread").closest(".text-truncate").css("font-weight", "normal");

					//$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight")}, 1000);

					//connect(username);
					//                     loadWebsocket();

					checkOnline(contactUserName);


				},
				error: function(e) {
					console.log(e);
					sessionTimeout(e);
					stopLoader();
				}
			});




			$("#typedChatText").on("keypress", function(event) {
				var keycode = (event.keyCode ? event.keyCode : event.which);
				if (keycode == '13') {
					if ($(".storeFile") != null && $(".storeFile").length > 0) {
						sendFiles()
					}

					if ($("#typedChatText").val() != null && $("#typedChatText").val() != "") {
						sendChatMessage();
						sendWebsocketMsg();
					}

				}
			})


			$(".chatContent").on("dragover", function(e) {
				e.preventDefault();
				e.stopPropagation();
				$("#dropzone").css("display", "block");
			});

			$(".chatContent").on("dragend", function(e) {
				e.preventDefault();
				e.stopPropagation();
				$("#dropzone").css("display", "none");
			});
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}



function sendChatMessage() {
	var fromUser = $("#ssUsername").val();
	var contactUserName = $("#contactUserName").val();
	var chatText = $("#typedChatText").val();


	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'json',
		url: 'insertChatMessage',
		cache: false,
		data: {
			contactUserName: contactUserName,
			chatText: chatText
		},
		success: function(response) {
			stopLoader();
			var insertCount = response['insertCount'];
			if (insertCount > 0) {

				var chatTextDiv = "<div class='col-12'>"
					+ "<div class='chatMessage fromChatMessage' ><span>" + chatText + "</span></div>"
					+ "<div class='userFromOptions'>"
					+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
					+ "</div>"
					+ "<div>";
				$(".contactChatLog").append(chatTextDiv);
				$("#typedChatText").val("");
				$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}

function checkNewMessages() {
	var openedContactUserName = $("#contactUserName").val();
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'json',
		url: 'checkNewMessages',
		cache: false,
		data: {
			openedContactUserName: openedContactUserName
		},
		success: function(response) {
			stopLoader();
			var contactChatLog = response['contactChatLog'];
			if (contactChatLog != null) {
				$(".contactChatLog").append(contactChatLog);
			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}

var timeOut;
function searchUsers() {
	clearTimeout(timeOut);
	timeOut = setTimeout(function() {

		var filterValue = $("#cm-search-users").val();
		showLoader();
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'json',
			url: 'fetchContactsList',
			cache: false,
			async: false,
			data: {
				filterValue: filterValue,
				startIndex: '0',
				endIndex: '20'
			},
			success: function(response) {
				stopLoader();
				var contactsList = response['contactsList'];
				$(".chat-list-div").html(contactsList);

				fetchUnreadMessageCount();

				$("#cmContactsListStartIndex").val("20");
				var timeout;
				$(".contactsList").unbind("scroll").on("scroll", function(event) {
					console.log("scrolled ");
					if ($(this).scrollTop() + $(this).innerHeight() >= ($(this)[0].scrollHeight - 2)) {
						console.log("iam in scroll functionality...........");
						clearTimeout(timeout);
						timeut = setTimeout(function() {
							loadContactsListOnScroll(event);
						}, 50);
					}
				});

			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}
		});

	}, 1000);
}


function loadContactsListOnScroll(event) {

	var filterValue = $("#cm-search-users").val();
	var startIndex = $("#cmContactsListStartIndex").val();
	var endIndex = "20";
	showLoader();
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'json',
		url: 'fetchContactsList',
		cache: false,
		async: false,
		data: {
			filterValue: filterValue,
			startIndex: startIndex,
			endIndex: endIndex,
			scrollEvent: 'Y'
		},
		success: function(response) {
			stopLoader();
			var contactsList = response['contactsList'];
			var contactsListSize = response['contactsListSize'];
			$(".contactsList").find('ul').append(contactsList);

			fetchUnreadMessageCount();


			var startInd = parseInt(startIndex) + contactsListSize;
			$("#cmContactsListStartIndex").val(startInd);

			//	var endInd = parseInt(endIndex) +contactsListSize;
			//	$("#cmContactsListEndIndex").val(endInd);

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});

}

function fetchUnreadMessageCount() {
	var filterValue = $("#cm-search-users").val();
	var startIndex = $("#cmContactsListStartIndex").val();
	var endIndex = "20";
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'json',
		url: 'fetchUnreadMessageCount',
		cache: false,
		async: false,
		data: {
			filterValue: filterValue,
			startIndex: startIndex,
			endIndex: endIndex
		},
		success: function(response) {
			stopLoader();

			var dataList = response['dataList'];
			if (dataList != null) {
				$.each(dataList, function(i) {
					var user = this[0];
					var msgCount = this[1];
					$("#" + user + "_unread").text(msgCount);
					$("#" + user + "_unread").closest(".text-truncate").css("font-weight", "bold");
				})
			}


		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}

function selectfile() {
	$("#selectfile").trigger("click");
}

var sendFilesArray;

function uploadAndSendFile(event) {
	if ($(".storeFile").length == 0) {
		sendFilesArray = [];
	}

	var files = event.target.files;
	if (files != null && files.length > 0) {
		$("#dropzone").css("display", "none");
		$(".fileStoreDiv").css("display", "flex");

		var file = event.target.files[0];
		sendFilesArray.push(file);

		if (file.type.indexOf("image") > -1) {
			/*var reader = new FileReader(file);
			reader.readAsDataURL(file);

			reader.onload = function(e) {
				//  $('.fileStoreDiv').append($('<img />').attr('src', e.target.result).fadeIn());
				var storeFileHtml = "<div class='storeFile'><img  src='" + e.target.result + "' />"
					+ "<span class='removeStoreFile' onclick='removeStoreFile(event)' aria-hidden='true'>×</span>"
					+ "</div></div>";
				$('.fileStoreDiv').append(storeFileHtml);
				$("#typedChatText").focus();
				reader.readAsArrayBuffer(file);
			}*/

			var reader = new FileReader();
			reader.onload = function(e) {
				var contents = e.target.result;

				/*         var blob = new Blob([this.result], {type: 'application/octet-stream'}); */
				var uint8Array = new Uint8Array(contents);

				var arrayBuffer = uint8Array.buffer;
				var blob = new Blob([arrayBuffer]);
				console.log(uint8Array);

				//video.src = URL.createObjectURL(blob);
				// $('.fileStoreDiv').append($('<img />').attr('src', e.target.result).fadeIn());
				var storeFileHtml = "<div class='storeFile'><img  src='" + URL.createObjectURL(blob) + "' />"
					+ "<span class='removeStoreFile' onclick='removeStoreFile(event)' aria-hidden='true'>×</span>"
					+ "</div></div>";
				$('.fileStoreDiv').append(storeFileHtml);
				$("#typedChatText").focus();

				console.log(contents);
				/*   video.src=contents */
			};
			reader.readAsArrayBuffer(file);


		} else if (file.type.indexOf("video") > -1) {
			var reader = new FileReader();
			reader.onload = function(e) {
				var contents = e.target.result;

				/*         var blob = new Blob([this.result], {type: 'application/octet-stream'}); */
				var uint8Array = new Uint8Array(contents);

				var arrayBuffer = uint8Array.buffer;
				var blob = new Blob([arrayBuffer]);
				console.log(uint8Array);

				//video.src = URL.createObjectURL(blob);
				var storeFileHtml = "<div class='storeFile'><video  controls src='" + URL.createObjectURL(blob) + "' ></video>"
					+ "<span class='removeStoreFile' onclick='removeStoreFile(event)' aria-hidden='true'>×</span>"
					+ "</div></div>";
				$('.fileStoreDiv').append(storeFileHtml);
				$("#typedChatText").focus();

				console.log(contents);
				/*   video.src=contents */
			};
			reader.readAsArrayBuffer(file);

		} else {
			
				var contents = file.type

				var storeFileHtml = "<div class='storeFile'><img src='images/cm/document.png' />"
					+ "<span class='removeStoreFile' onclick='removeStoreFile(event)' aria-hidden='true'>×</span>"
					+ "</div></div>";
				$('.fileStoreDiv').append(storeFileHtml);
				$("#typedChatText").focus();

				console.log(contents);
				/*   video.src=contents */


		}


	}


};

function sendFiles() {
	$(".storeFile").remove();
	$('.fileStoreDiv').hide();
	if (sendFilesArray != null && sendFilesArray.length > 0) {
		$.each(sendFilesArray, function(index) {

			var messageId = genHexString(32);
			var file = sendFilesArray[index];
			var fileType = file.type;
			var fileName = file.name;
			var toUser = $("#contactUserName").val();
			var fromUser = $("#ssUsername").val();
			var fileData = new FormData();
			fileData.append("fileContent", file);
			fileData.append("filetype", fileType);
			fileData.append("toUser", toUser);
			fileData.append("messageId", messageId);

			$.ajax({
				url: 'saveFileTODB',
				type: "POST",
				data: fileData,
				enctype: 'multipart/form-data',
				processData: false,
				contentType: false,
				async: false,
				success: function(response) {
					stopLoader();
					if (response != null) {
						var insertCount = response['insertCount'];
						if (insertCount != null && insertCount > 0) {
							sendWebsocketMsg(fileType, messageId);

							var response = fetchFileFromDB(fromUser, toUser, fileType, messageId);
							var blobfile = response['blobfile'];

							if (blobfile != null) {
								if (fileType.indexOf("video") > -1) {

									var blob = base64ToBlob(blobfile, fileType);

									// video.src = URL.createObjectURL(blob);

									var chatTextDiv = "<div class='col-12'>"
										+ "<div class='chatMessage fromChatMessage' ><video width='200px' height='150px' controls src='" + URL.createObjectURL(blob) + "' ></video></div>"
										+ "<div class='userFromOptions'>"
										+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
										+ "</div>"
										+ "<div>";
									$(".contactChatLog").append(chatTextDiv);
									$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);

								} else if (fileType.indexOf("image") > -1) {

									var blob = base64ToBlob(blobfile, fileType);

									var chatTextDiv = "<div class='col-12'>"
										+ "<div class='chatMessage fromChatMessage' ><img width='200px' height='150px' src='" + URL.createObjectURL(blob) + "' class=''  alt='userProfile'></div>"
										+ "<div class='userFromOptions'>"
										+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
										+ "</div>"
										+ "<div>";
									$(".contactChatLog").append(chatTextDiv);
									$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);
								} else {

									// var blob = base64ToBlob(blobfileString, contentType);

									var chatTextDiv = "<div class='col-12'>"
										+ "<div class='chatMessage fromChatMessage' ><div ><span>" + fileName + "</span>"
										+ "<img width='50px' height='50px' src='images/cm/downloadFile.png' class='' onclick=downloadCMFile(event, '" + messageId + "', '" + fileType + "', '" + fromUser + "', '" + toUser + "')  ></div></div>"
										+ "<div class='userFromOptions'>"
										+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
										+ "</div>"
										+ "<div>";
									$(".contactChatLog").append(chatTextDiv);
									$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);


								}

							}

						}
					}

				}, error: function(e) {
					console.log("The Error Message is:::" + e.message);
					sessionTimeout(e);
				}
			});

		})
		sendFilesArray = [];
	}

}

function removeStoreFile(event) {

	$(event.target).closest(".storeFile").remove();

	if ($(".storeFile").length == 0) {
		$(".fileStoreDiv").css("display", "none");
	}
}

function fetchFileFromDB(fromUser, toUser, fileType, messageId) {
	var responseObj;
	$.ajax({
		type: "post",
		traditional: true,
		dataType: 'json',
		url: 'fetchFileFromDB',
		cache: false,
		async: false,
		data: {
			fromUser: fromUser,
			toUser: toUser,
			fileType: fileType,
			messageId: messageId
		},
		success: function(response) {
			stopLoader();
			responseObj = response;

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
	return responseObj;
}

function base64ToBlob(blobfileString, fileType) {

	const byteCharacters = atob(blobfileString);
	const byteNumbers = new Array(byteCharacters.length);
	for (let i = 0; i < byteCharacters.length; i++) {
		byteNumbers[i] = byteCharacters.charCodeAt(i);
	}
	const byteArray = new Uint8Array(byteNumbers);
	const blob = new Blob([byteArray], { type: fileType });
	return blob;
}

function dowmloadAndPlayVideo(event, messageId, fileType, fromUser, toUser) {
	var response = fetchFileFromDB(fromUser, toUser, fileType, messageId);
	var blobfileString = response['blobfile'];
	var blob = base64ToBlob(blobfileString)
	$(event.target).hide();
	$(event.target).closest('.chatMessage').find('video').attr("src", URL.createObjectURL(blob));
	$(event.target).closest('.chatMessage').find('video')[0].play();
}

function downloadCMFile(event, messageId, fileType, fromUser, toUser) {
	var response = fetchFileFromDB(fromUser, toUser, fileType, messageId);
	var blobfileString = response['blobfile'];
	var blob = base64ToBlob(blobfileString);
	var a = document.createElement("a");
	document.body.appendChild(a);
	a.style = "display: none";
	url = window.URL.createObjectURL(blob);
	a.href = url;
	a.download = 'download';
	a.click();
	window.URL.revokeObjectURL(url);
}

