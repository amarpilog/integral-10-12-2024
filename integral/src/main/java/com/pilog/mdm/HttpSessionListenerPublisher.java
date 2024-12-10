//package com.pilog.mdm;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import javax.servlet.http.HttpSession;
//import javax.servlet.http.HttpSessionEvent;
//
//import org.springframework.security.web.session.HttpSessionEventPublisher;
//import org.springframework.stereotype.Component;
//
///*
// * To change this license header, choose License Headers in Project Properties.
// * To change this template file, choose Tools | Templates
// * and open the template in the editor.
// */
//
///**
// *
// * @author pilog
// */
//@Component
//public class HttpSessionListenerPublisher extends HttpSessionEventPublisher{
//    
//  
////   private static final Logger logger = LoggerFactory.getLogger(HttpSessionListenerPublisher.class);
//    
//       private static int count = 0;
////  HttpSession session;
//  public static Map<String, HttpSession> sessions = new HashMap();
//    public static List removedsessions = new ArrayList();
//  
//  public  int getSessionCount()
//  {
//     // ////logger.info("MetaProperties::getSessionCount::::");
//    return count;
//  }
//  
//    @Override
//   public void sessionCreated(HttpSessionEvent event) {
//      super.sessionCreated(event);
//      
//     String username = (String)event.getSession().getAttribute("ssUsername");
//     //logger.info("sessionCreated::username::"+username);
////        if (username !=null && !"".equalsIgnoreCase(username)) {
//             //logger.info("sessionCreated::Date::"+new VisionUtills().getCurrentTimeZoneData((String)request.getSession(false).getAttribute("ssTimeZone")));
//             sessions.put(event.getSession().getId(), event.getSession());
////             this.session = event.getSession();
//            count += 1;
////        }
////   logger.info("sessions:sessionCreated::"+sessions);
//   
//   }
//
//   @Override
//   public void sessionDestroyed(HttpSessionEvent event) {
//     
//       //logger.info("sessionDestroyed::Date::"+new VisionUtills().getCurrentTimeZoneData((String)request.getSession(false).getAttribute("ssTimeZone")));
//        count -= 1;
////       logger.info("sessions:sessionDestroyed::" + sessions);
//     removedsessions.add(event.getSession().getId());
//     sessions.remove(event.getSession().getId());
//   // this.session = event.getSession();
//   // String username = (String)this.session.getAttribute("ssUsername");
////       logger.info("removedsessions::::"+removedsessions);
////       logger.info("sessions::::"+sessions);
//      super.sessionDestroyed(event);
//      
//      
//   }
//    
////   public HttpSession getExpriredSession()
////   {
////       
////       return this.session;
////   
////   }
//  
//}
