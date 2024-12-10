package com.pilog.mdm;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class MyController {

@Autowired
SessionFactory sessionFactory; 

	 @RequestMapping("/hello")
	public String sayHello(HttpServletRequest request) {
		HttpSession httpsession = request.getSession(false);
		System.out.println("httpsession"+httpsession);
		return "Hello, Application is running on JBOSS EAP 7.2"+httpsession;

	}

}
