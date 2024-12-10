package com.pilog.mdm.websocket;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.imageio.ImageIO;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;


@ServerEndpoint("/sendfile/{username}")
public class BinaryWebSocketServer {
   // private  static final Set<Session> sessions =  Collections.synchronizedSet(new HashSet<Session>());
   // private Session session;
	private static final Set<ChatEndpoint> chatEndpoints = new CopyOnWriteArraySet<>();
	private static final Set<Session> sessions = Collections.synchronizedSet(new HashSet<Session>());
	private static HashMap<String, String> users = new HashMap<>();


    @OnOpen
    public void onOpen(Session session,  @PathParam("username") String username) {
        sessions.add(session);
        users.put(username, session.getId());
        System.out.println("onOpen_File::" + session.getId());        
    }
    @OnClose
    public void onClose(Session session,  @PathParam("username") String username) {
        sessions.remove(session);
        users.remove(username);
        System.out.println("onClose_File::" +  session.getId());
    }

    @OnMessage
    public void onMessage(ByteBuffer data, boolean last, Session session) {
//        System.out.println("onByteArrayMessage::From=" + session.getId() + " with len:" + data.length );
//        ByteArrayInputStream bis = new ByteArrayInputStream(data);
//        BufferedImage bImage2;
        try {
//            bImage2 = ImageIO.read(bis);
//             ImageIO.write(bImage2, "jpg", new File("output.jpg") );
             session.getBasicRemote().sendBinary(data);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        System.out.println("image created");

    }


    @OnError
    public void onError(Throwable t) {
        System.out.println("onError::" + t.getMessage());
    }

}