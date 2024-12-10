//package com.pilog.mdm;
//
//import java.util.ArrayList;
//
//import javax.websocket.*;
//import javax.websocket.server.ServerEndpoint;
//
//import org.json.simple.JSONObject;
//import org.json.simple.JSONValue;
//
//import java.util.List;
//
//@ServerEndpoint(value = "/hello")
//
//public class HelloWorldEndpoint {
//	List users = new ArrayList();
//
//	@OnMessage
//	public JSONObject hello(Session session, String message) {
//		   System.out.println("username");
//				JSONObject returnObj = new JSONObject();
//	
//				        JSONObject data = (JSONObject)JSONValue.parse(message);
//
//				        JSONObject user = (JSONObject)findUser(String.valueOf(data.get("username")) );
//
//				
//				        String dataType = (String)data.get("type");
//				        if (dataType.equalsIgnoreCase("store_user")) {
//				        	 if (user != null &&  !user.isEmpty()) {
//				                    return null;
//				                }
//
//				        	 JSONObject newUser =  new JSONObject();
//				        	 newUser.put("username", data.get("username"));
//				        	 newUser.put("conn", session);
//				        	 users.add(newUser);
//		
//				                System.out.println(newUser.get("username"));
//				        }
//				        
//			
//				        if (dataType.equalsIgnoreCase("store_offer")) {
//				        	 if (user == null) {
//				                    return null;
//				                }
//
//				        	 user.put("offer", data.get("offer"));
//				        
//				        }
//
//				        if (dataType.equalsIgnoreCase("store_candidate")) {
//				        	 if (user == null) {
//				                    return null;
//				                }
//				        	 if (user.get("candidates") == null) {
//				        		 user.put("candidates", new ArrayList()) ;
//				        	 }
//				        	 ((ArrayList)user.get("candidates")).add(data.get("candidate"));
//				        	
//				        
//				        }
//				        if (dataType.equalsIgnoreCase("send_answer")) {
//				        	 if (user == null) {
//				                    return null;
//				                }
//				        	 JSONObject ansObj = new JSONObject();
//				        	 ansObj.put("type", "answer");
//				        	 ansObj.put("answer", data.get("answer"));
////				        	 sendData(ansObj,  user.get("conn"));
//				        	 returnObj = ansObj;
//				        	
//				        
//				        }
//				        if (dataType.equalsIgnoreCase("send_candidate")) {
//				        	 if (user == null) {
//				                    return null;
//				                }
//				        	 JSONObject candidateObj = new JSONObject();
//				        	 candidateObj.put("type", "candidate");
//				        	 candidateObj.put("candidate", data.get("candidate"));
////				        	 sendData(candidateObj,  user.get("conn"));
//				        	 returnObj = candidateObj;
//				        	
//				        
//				        }
//				        if (dataType.equalsIgnoreCase("join_call")) {
//				        	 if (user == null) {
//				                    return null;
//				                }
//				        	 JSONObject obj = new JSONObject();
//				        	 obj.put("type", "offer");
//				        	 obj.put("offer", user.get("offer"));
////				        	 sendData(obj,  session);
//				        	 returnObj = obj;
//				        
//				        }
//				
//				
//		System.out.println("Received : "+ message);
////		return "Hello form the Server!";
//		return returnObj;
//	}
//
//	@OnOpen
//	public void myOnOpen(Session session) {
//		System.out.println("WebSocket opened: " + session.getId());
//	}
//
//	@OnClose
//	public void myOnClose(CloseReason reason) {
//		System.out.println("Closing a due to " + reason.getReasonPhrase());
//	}
//
//	@OnError
//	public void error(Throwable t) {
//
//	}
//
//	public JSONObject findUser(String username) {
//		JSONObject user = new JSONObject();
//	    for (int i = 0;i < users.size();i++) {
//	    	JSONObject userObj = (JSONObject)users.get(i);
//	    	String userObjectUser = String.valueOf(userObj.get("username"));
//	        if ( userObjectUser!=null && userObjectUser.equalsIgnoreCase(username)) {
//	        	  user = (JSONObject)users.get(i);
//	        }
//	         
//	    }
//	    return user;
//	}
//
////	public void sendData(JSONObject data, Session session) {
////		session.send(JSON.stringify(data))
////	}
//    
////    connection.on('close', (reason, description) => {
////        users.forEach(user => {
////            if (user.conn == connection) {
////                users.splice(users.indexOf(user), 1)
////                return
////            }
////        })
////    })
//}
