package com.pilog.mdm.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.web.session.HttpSessionEventPublisher;
import org.springframework.stereotype.Service;

@Service
public class HttpSessionListenerPublisher extends HttpSessionEventPublisher {
  private static final Logger logger = LoggerFactory.getLogger(HttpSessionListenerPublisher.class);
  
  private static int count = 0;
  
  public static Map<String, HttpSession> sessions = new HashMap<>();
  
  public static List removedsessions = new ArrayList();
  
  public int getSessionCount() {
    return count;
  }
  
  public void sessionCreated(HttpSessionEvent event) {
    super.sessionCreated(event);
    String username = (String)event.getSession().getAttribute("ssUsername");
    sessions.put(event.getSession().getId(), event.getSession());
    count++;
  }
  
  public void sessionDestroyed(HttpSessionEvent event) {
    count--;
    removedsessions.add(event.getSession().getId());
    sessions.remove(event.getSession().getId());
    super.sessionDestroyed(event);
  }
}