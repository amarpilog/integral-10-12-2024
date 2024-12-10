//connecting to our signaling server 

var conn;
var peerConnection;
var dataChannel;
var username;
var tousername;
var localStream;
function loadWebsocket() {

    var ssUsername = $("#ssUsername").val();
    username = ssUsername;
    var host = document.location.host;
	var pathname = document.location.pathname;
    conn = new WebSocket('wss://'+ host +'/insights/socket');
    
    conn.onopen = function () {
        console.log("Connected to the signaling server");
        initialize(ssUsername);
    };

    conn.onmessage = function (msg) {
        console.log("Got message", msg.data);
        var content = JSON.parse(msg.data);
        var data = content.data;
        switch (content.event) {
            // when somebody wants to call us
            case "offer":
                var type = content.type;
                var fromusername = content.username;
                handleOffer(data, type, fromusername);
                break;
            case "answer":
                handleAnswer(data);
                break;
                // when a remote peer sends an ice candidate to us
            case "candidate":
                handleCandidate(data);
                break;
            default:
                break;
        }
    };
}

function send(message) {
    message.username = username;
    message.tousername = tousername;
    conn.send(JSON.stringify(message));
}


function initialize(fromuser) {
    var userObj = {};
    userObj['username'] = fromuser;
    conn.send(JSON.stringify(userObj));

    let configuration = {
        iceServers: [
            {
                "urls": ["stun:stun.l.google.com:19302",
                    "stun:stun1.l.google.com:19302",
                    "stun:stun2.l.google.com:19302"]
            }
        ]
    }

    peerConnection = new RTCPeerConnection(configuration)

    peerConnection.onicecandidate = function (event) {
        if (event.candidate) {
            send({
                event: "candidate",
                data: event.candidate
            });
        }
    };

//    // creating data channel
//    dataChannel = peerConnection.createDataChannel("dataChannel", {
//        reliable: true
//    });
//
//    dataChannel.onerror = function (error) {
//        console.log("Error occured on datachannel:", error);
//    };
//
//    // when we receive a message from the other peer, printing it on the console
//    dataChannel.onmessage = function (event) {
//        console.log("message:", event.data);
//        var chatTextDiv = "<div class='col-12'>"
//                + "<div class='chatMessage toChatMessage' ><span>" + event.data + "</span></div>"
//                + "</div>";
//        $(".contactChatLog").append(chatTextDiv);
//    };
//
//    dataChannel.onclose = function () {
//        console.log("data channel is closed");
//    };
//
//    peerConnection.ondatachannel = function (event) {
//        dataChannel = event.channel;
//    };

}

function createOffer(offerType) {
    peerConnection.createOffer(function (offer) {
        send({
            event: "offer",
            data: offer,
            type: offerType
        });
        peerConnection.setLocalDescription(offer);
    }, function (error) {
        alert("Error creating an offer");
    });
}

function sendMessage() {
    var input = document.getElementById("typedChatText");
    dataChannel.send(input.value);
    input.value = "";
}

function sendUsername(callerName) {
    username = callerName;
    send({
        event: "user"
    })
}

function startCall(fromuser, touser) {
    tousername = touser;
    $(".outgoingCall").show();
    $("#outgoingCallUserId").text(touser);
    //sendUsername(callerName);

    document.getElementById("video-call-div")
            .style.display = "inline"

    navigator.getUserMedia({
        video: {
            frameRate: 24,
            width: {
                min: 480, ideal: 720, max: 1280
            },
            aspectRatio: 1.33333
        },
        audio: true
    }, (stream) => {
        localStream = stream
        document.getElementById("local-video").srcObject = localStream

        let configuration = {
            iceServers: [
                {
                    "urls": ["stun:stun.l.google.com:19302",
                        "stun:stun1.l.google.com:19302",
                        "stun:stun2.l.google.com:19302"]
                }
            ]
        }

        peerConnection = new RTCPeerConnection(configuration)
        peerConnection.addStream(localStream)

        peerConnection.onicecandidate = function (event) {
            if (event.candidate) {
                send({
                    event: "candidate",
                    data: event.candidate
                });
            }
        };

        peerConnection.onaddstream = (event) => {
            document.getElementById("remote-video")
                    .srcObject = event.stream
        }

        // creating data channel
//        dataChannel = peerConnection.createDataChannel("dataChannel", {
//            reliable: true
//        });
//
//        dataChannel.onerror = function (error) {
//            console.log("Error occured on datachannel:", error);
//        };
//
//        // when we receive a message from the other peer, printing it on the console
//        dataChannel.onmessage = function (event) {
//            console.log("message:", event.data);
//        };
//
//        dataChannel.onclose = function () {
//            console.log("data channel is closed");
//        };
//
//        peerConnection.ondatachannel = function (event) {
//            dataChannel = event.channel;
//        };

        createOffer("calleroffer");


    }, (error) => {
        console.log(error)
    })

}

function joinCall() {
    tousername = $("#incommingCallUserId").text();
    $(".incommingCall").hide();

    document.getElementById("video-call-div")
            .style.display = "inline"

    navigator.getUserMedia({
        video: {
            frameRate: 24,
            width: {
                min: 480, ideal: 720, max: 1280
            },
            aspectRatio: 1.33333
        },
        audio: true
    }, (stream) => {
        localStream = stream
        document.getElementById("local-video").srcObject = localStream

        let configuration = {
            iceServers: [
                {
                    "urls": ["stun:stun.l.google.com:19302",
                        "stun:stun1.l.google.com:19302",
                        "stun:stun2.l.google.com:19302"]
                }
            ]
        }

        peerConnection = new RTCPeerConnection(configuration)
        peerConnection.addStream(localStream)

        peerConnection.onicecandidate = function (event) {
            if (event.candidate) {
                send({
                    event: "candidate",
                    data: event.candidate
                });
            }
        };

        peerConnection.onaddstream = (event) => {
            document.getElementById("remote-video")
                    .srcObject = event.stream
        }

        // creating data channel
//        dataChannel = peerConnection.createDataChannel("dataChannel", {
//            reliable: true
//        });
//
//        dataChannel.onerror = function (error) {
//            console.log("Error occured on datachannel:", error);
//        };
//
//        // when we receive a message from the other peer, printing it on the console
//        dataChannel.onmessage = function (event) {
//            console.log("message:", event.data);
//        };
//
//        dataChannel.onclose = function () {
//            console.log("data channel is closed");
//        };
//
//        peerConnection.ondatachannel = function (event) {
//            dataChannel = event.channel;
//        };

        createOffer("receiveroffer");

    }, (error) => {
        console.log(error)
    })
}

function handleOffer(offer, type, fromusername) {
    if (type == "calleroffer") {
        $(".incommingCall").show();
        $("#incommingCallUserId").text(fromusername);
    } else if (type == "receiveroffer") {
        $(".outgoingCall").hide();
    } else if (type == "callerHanged") {
        closeCall('incommingCall');
        return false;
    } else if (type == "receiverHanged") {
        closeCall('outgoingCall');
        return false;
    }
    peerConnection.setRemoteDescription(new RTCSessionDescription(offer));
    peerConnection.createAnswer(function (answer) {
        peerConnection.setLocalDescription(answer);
        send({
            event: "answer",
            data: answer
        });
    }, function (error) {
        alert("Error creating an answer");
    });

    // create and send an answer to an offer
}
;

function handleCandidate(candidate) {
    peerConnection.addIceCandidate(new RTCIceCandidate(candidate));
}
;

function handleAnswer(answer, type) {

    peerConnection.setRemoteDescription(new RTCSessionDescription(answer));
    console.log("connection established successfully!!");
}
;

let isAudio = true
function muteAudio() {
    isAudio = !isAudio
    localStream.getAudioTracks()[0].enabled = isAudio
    if (isAudio) {
        $("#audioAction").attr("src", "images/cm/microphone.png");
    } else {
        $("#audioAction").attr("src", "images/cm/muteAudio.png");
    }
}

let isVideo = true
function muteVideo() {
    isVideo = !isVideo
    localStream.getVideoTracks()[0].enabled = isVideo
    if (isVideo) {
        $("#videoAction").attr("src", "images/cm/camera.png");
    } else {
        $("#videoAction").attr("src", "images/cm/muteCamera.png");
    }
}


function hangCall(divclass, create) {
    if (create == "Y" && divclass == "incommingCall") {
        tousername = $("#incommingCallUserId").text();
        createOffer("receiverHanged");
        //return false;
    } else if (create == "Y" && divclass == "outgoingCall") {
        createOffer("callerHanged");
        //return false;
    }
    if (localStream != null) {
        const tracks = localStream.getTracks();
        tracks.forEach(track => track.stop());
    }

    document.getElementById("video-call-div").style.display = "none"
    $("." + divclass).hide();
    setTimeout(function () {
         peerConnection.close();
        initialize(username);
    }, 1000)

//    document.getElementById("local-video").src = '';
//    document.getElementById("remote-video").src = '';

}

function closeCall(divclass) {
    if (localStream != null) {
        const tracks = localStream.getTracks();
        tracks.forEach(track => track.stop());
    }
    document.getElementById("video-call-div").style.display = "none"
    if (divclass != null) {
        $("." + divclass).hide();
    }
    peerConnection.close();
    initialize(username);

}
