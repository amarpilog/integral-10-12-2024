var ws;

function connect(username) {
	//var username = document.getElementById("username").value;

	var host = document.location.host;
	var pathname = document.location.pathname;
	
	ws = new WebSocket("wss://" + host + "/insights/chat/" + username);

	ws.onmessage = function(event) {
		var log = document.getElementById("log");
		console.log(event.data);
		var message = JSON.parse(event.data);
		var content = message.content;
		var fromUser = message.fromUser;
		var toUser = message.toUser;
		var contentType = message.contentType;
		var messageId = message.messageId;
		
		if (contentType!=null) {
			if ($("#contactUserName").val() != null && $("#contactUserName").val() == fromUser) {
					
					var response = fetchFileFromDB(fromUser, toUser, contentType, messageId);
					if (response!=null) {
					
						var blobfile = response['blobfile'];
						
						if (blobfile!=null){
								if (contentType.indexOf("video") > -1) {
								
									var blob = base64ToBlob(blobfileString, contentType);
								
								     // video.src = URL.createObjectURL(blob);
								      
									var chatTextDiv = "<div class='col-12'>"
										+ "<div class='chatMessage toChatMessage' ><video width='200px' height='150px' controls src='" + URL.createObjectURL(blob) + "' ></video></div>"
										+ "<div class='userToOptions'>"
										+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
										+ "</div>"
										+ "<div>";
									$(".contactChatLog").append(chatTextDiv);
									$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);
									
								} else if (contentType.indexOf("image") > -1) {
									
									var blob = base64ToBlob(blobfileString, contentType);
							
									var chatTextDiv = "<div class='col-12'>"
										+ "<div class='chatMessage toChatMessage' ><img width='200px' height='150px' src='" + URL.createObjectURL(blob) + "' class=''  alt='userProfile'></div>"
										+ "<div class='userToOptions'>"
										+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
										+ "</div>"
										+ "<div>";
									$(".contactChatLog").append(chatTextDiv);
									$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);
								} else {
									
									var blob = base64ToBlob(blobfileString, contentType);
								
									var chatTextDiv = "<div class='col-12'>"
										+ "<div class='chatMessage toChatMessage' ><div ><span>"+content+"</span>"
										+"<img width='50px' height='50px' src='images/cm/downloadFile.png' class='' onclick=downloadCMFile(event, '"+messageId+"', '"+contentType+"', '"+fromUser+"', '"+toUser+"')  ></div></div>"
										+ "<div class='userToOptions'>"
										+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
										+ "</div>"
										+ "<div>";
									$(".contactChatLog").append(chatTextDiv);
									$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);
								}
								
							} 
					
					/*	var chatTextDiv = "<div class='col-12'>"
						+ "<div class='chatMessage toChatMessage' ><img src='data:"+contentType+";base64,"+blob+"' class='' width='100px' alt='userProfile'></div>"
						+ "<div class='userToOptions'>"
						+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
						+ "</div>"
						+ "<div>";
					$(".contactChatLog").append(chatTextDiv);
					$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);*/
						
					}
					

				} else if ($("#" + fromUser + "_unread").length > 0) {

					var unreadCount = parseInt($("#" + fromUser + "_unread").text());
					if (unreadCount != null && !isNaN(unreadCount)) {
						$("#" + fromUser + "_unread").text(unreadCount + 1);
					} else {
						$("#" + fromUser + "_unread").text(1);
					}
					$("#" + fromUser + "_unread").closest(".text-truncate").css("font-weight", "bold");

				}
		} else {
			if (content != null && content != "" && content == "!OnlineConnected!") {

				if ($("#contactUserName").val() != null && $("#contactUserName").val() == fromUser) {
					$("#onlineStatus").text("Online");
				}
			} else if (content != null && content != "" && content == "!OnlineDisconnected!") {
				if ($("#contactUserName").val() != null && $("#contactUserName").val() == fromUser) {
					$("#onlineStatus").text("");
				}
			} else if (content != null && content != "") {

				if ($("#contactUserName").val() != null && $("#contactUserName").val() == fromUser) {
					var chatTextDiv = "<div class='col-12'>"
						+ "<div class='chatMessage toChatMessage' ><span>" + content + "</span></div>"
						+ "<div class='userToOptions'>"
						+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
						+ "</div>"
						+ "<div>";
					$(".contactChatLog").append(chatTextDiv);
					$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight") }, 1000);

				} else if ($("#" + fromUser + "_unread").length > 0) {

					var unreadCount = parseInt($("#" + fromUser + "_unread").text());
					if (unreadCount != null && !isNaN(unreadCount)) {
						$("#" + fromUser + "_unread").text(unreadCount + 1);
					} else {
						$("#" + fromUser + "_unread").text(1);
					}
					$("#" + fromUser + "_unread").closest(".text-truncate").css("font-weight", "bold");

				}
			}
		}

	};
}

function sendWebsocketMsg(contentType, messageId) {
	var fromUser = $("#ssUsername").val();
	var toUser = $("#contactUserName").val();
	//var content = document.getElementById("msg").value;
	var content = $("#typedChatText").val();
	
	var json = JSON.stringify({
		"fromUser": fromUser,
		"toUser": toUser,
		"content": content,
		"contentType":contentType,
		"messageId":messageId
	});

	ws.send(json);
}

function checkOnline() {
	var fromUser = $("#ssUsername").val();
	var toUser = $("#contactUserName").val();
	var json = JSON.stringify({
		"fromUser": fromUser,
		"toUser": toUser,
		"content": "!OnlineConnected!"
	});
	ws.send(json);
}



function genHexString(len) {
    const hex = '0123456789ABCDEF';
    let output = '';
    for (let i = 0; i < len; ++i) {
        output += hex.charAt(Math.floor(Math.random() * hex.length));
    }
    return output;
}