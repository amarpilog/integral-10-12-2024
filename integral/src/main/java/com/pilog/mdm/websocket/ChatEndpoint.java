package com.pilog.mdm.websocket;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.websocket.EncodeException;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.pilog.mdm.model.Message;

@ServerEndpoint(value = "/chat/{username}", decoders = MessageDecoder.class, encoders = MessageEncoder.class)
public class ChatEndpoint {
	private Session session;
	private static final Set<ChatEndpoint> chatEndpoints = new CopyOnWriteArraySet<>();
	private static final Set<Session> sessions = new CopyOnWriteArraySet<>();
	private static HashMap<String, String> users = new HashMap<>();

	@OnOpen
	public void onOpen(Session session, @PathParam("username") String username) throws IOException, EncodeException {

		this.session = session;
		chatEndpoints.add(this);
		sessions.add(session);
		users.put(username, session.getId());

		Message message = new Message();
		message.setFromUser(username);
		message.setContent("!OnlineConnected!");
		sessions.forEach(sess -> {
			synchronized (session) {
				try {
					sess.getBasicRemote().sendObject(message);
					System.out.println(message.toString());
				} catch (IOException | EncodeException e) {
					e.printStackTrace();
				}
			}
		});
	}

	@OnMessage
	public void onMessage(Session session, Message message) throws IOException, EncodeException {

		broadcast(session, message);
	}

	@OnClose
	public void onClose(Session session, @PathParam("username") String username) throws IOException, EncodeException {

		Message message = new Message();
		message.setFromUser(username);
		message.setContent("!OnlineDisconnected!");
		sessions.forEach(sess -> {
			synchronized (sess) {
				try {
					sess.getBasicRemote().sendObject(message);
					System.out.println(message.toString());
				} catch (IOException | EncodeException e) {
					e.printStackTrace();
				}
			}
		});
		chatEndpoints.remove(this);
		sessions.remove(session);
		users.remove(username);
	}

	@OnError
	public void onError(Session session, Throwable throwable) {
		// Do error handling here
	}

	private static void broadcast(Session currentUserSession, Message message) throws IOException, EncodeException {
		/*
		 * chatEndpoints.forEach(endpoint -> { synchronized (endpoint) { try {
		 * endpoint.session.getBasicRemote() .sendObject(message);
		 * System.out.println(message.toString()); } catch (IOException |
		 * EncodeException e) { e.printStackTrace(); } } });
		 */
		
		String messageStr = message.toString();
//		JSONObject messageObj = (JSONObject)JSONValue.parse(messageStr);
//		String content = (String)messageObj.get("content");
		String content = message.getContent().toString();
		String contentType = message.getContentType();
		String fromUser = message.getFromUser();
		String toUser = message.getToUser();
		String toUserSessionId = users.get(toUser);
		if ( content != null && !"".equalsIgnoreCase(content)
				&& ("!OnlineConnected!".equalsIgnoreCase(content) || "!OnlineDisconnected!".equalsIgnoreCase(content))
				&& toUserSessionId != null ){
			try {
				message.setFromUser(toUser);
				message.setToUser(fromUser);
				String sessionId = currentUserSession.getId();
				currentUserSession.getBasicRemote().sendObject(message);
				System.out.println(message.toString());
			} catch (IOException | EncodeException e) {
				e.printStackTrace();
			}

		} else if ( (content != null && !"".equalsIgnoreCase(content) && toUserSessionId != null) 
				||  (contentType!=null && !"".equalsIgnoreCase(contentType) && !"null".equalsIgnoreCase(contentType) ) ) {
			sessions.forEach(session -> {
				synchronized (session) {
					try {
						String sessionId = session.getId();
						if (toUserSessionId.equalsIgnoreCase(sessionId)) {
							session.getBasicRemote().sendObject(message);
							System.out.println(message.toString());
						}
					} catch (IOException | EncodeException e) {
						e.printStackTrace();
					}
				}
			});
		}

	}
}
