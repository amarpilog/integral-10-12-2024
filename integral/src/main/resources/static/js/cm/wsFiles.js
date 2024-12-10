var webSocket;
function connectWSFiles(username) {
	var host = document.location.host;
	var pathname = document.location.pathname;

	webSocket = new WebSocket("wss://" + host + "/insights/sendfile/" + username);
	
	webSocket.binaryType = 'arraybuffer';
	webSocket.onopen = function(message) { 
		wsOpen(message); 
		};
	webSocket.onmessage = function(message){
		 wsGetMessage(message);
		 };
	webSocket.onclose = function(message) {
		 wsClose(message); 
		 };
	webSocket.onerror = function(message) { 
		wsError(message); 
		};
		
	function wsOpen(message) {
		console.log("Connected ...");
	}
	function wsSendMessage() {
		webSocket.send(message.value);
		console.log("Message sended to the server : " + message.value);
		message.value = "";
	}
	function wsCloseConnection() {
		webSocket.close();
	}
	function wsGetMessage(message) {
	
		console.log("Message received from to the server : " + message.data);
	
		var imgobj = toBase64(message.data);
		var chatTextDiv = "<div class='col-12'>"
					+"<div class='chatMessage toChatMessage' ><img src='data:image/png;base64,"+imgobj+"'></div>"
					+ "<div class='userToOptions'>"
					+ "<img src='images/cm/threedots-ver.png' class='' width='15px' alt='userProfile'>"
					+ "</div>"
					+"<div>";
				$(".contactChatLog").append(chatTextDiv);
				$('.contactChatLog').animate({ scrollTop: $('.contactChatLog').prop("scrollHeight")}, 1000);
	}
	function wsClose(message) {
		console.log("Disconnect ...");
	}

	function wsError(message) {
		console.log("Error ..." + message.code);
	}
}


function sendFile(file) {

	//var file = document.getElementById('filename').files[0];

	var reader = new FileReader();

	var rawData = new ArrayBuffer();

	reader.loadend = function(e) {

	};

	reader.onload = function(e) {

		var rawData = e.target.result;
		var byteArray = new Uint8Array(rawData);
		var fileByteArray = [];

		webSocket.send(byteArray.buffer);

		console.log("the File has been transferred.");

	};

	reader.readAsArrayBuffer(file);

}

function toBase64(arr) {
   arr = new Uint8Array(arr)  // if it's an ArrayBuffer
   return btoa(
      arr.reduce((data, byte) => data + String.fromCharCode(byte), '')
   );
}