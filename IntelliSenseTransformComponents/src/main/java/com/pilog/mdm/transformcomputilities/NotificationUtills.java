package com.pilog.mdm.transformcomputilities;

import com.pilog.mdm.access.DataAccess;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;



@Component
public class NotificationUtills {
	
	@Autowired
	private DataAccess access;
	
	public void sendMailNotification(String toEmail, String subject, String body) {
		
		try {
			String dalMailQuery = "SELECT "
					+ "SMTP_HOST, " //0
					+ "SMTP_STARTTLS_ENABLE, "//1
					+ "SMTP_AUTH, "//2
					+ "SMTP_PORT, "//3
					+ "USER_NAME, "//4
					+ "PASWORD "//5
					+ "FROM DAL_MAIL_CONFIG";
				
		List<Object []> mailConfigList = access.sqlqueryWithParams(dalMailQuery, Collections.EMPTY_MAP);
		
		if (mailConfigList!=null && !mailConfigList.isEmpty()) {
			Object [] rowdata = mailConfigList.get(0);
			final String userName = String.valueOf(rowdata[4]);
			final String fromEmail = String.valueOf(rowdata[4]);
			final String password = String.valueOf(rowdata[5]);

			System.out.println("TLSEmail Start");
			Properties props = new Properties();
			props.put("mail.smtp.host", String.valueOf(rowdata[0])); // SMTP Host
			props.put("mail.smtp.port", String.valueOf(rowdata[3])); // TLS Port
			props.put("mail.smtp.auth", String.valueOf(rowdata[2])); // enable authentication
			props.put("mail.smtp.starttls.enable", String.valueOf(rowdata[1])); // enable STARTTLS

			
			// create Authenticator object to pass in Session.getInstance argument
			Authenticator auth = new Authenticator() {
				// override the getPasswordAuthentication method
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(userName, password);
				}
			};
			Session session = Session.getInstance(props, auth);

			sendEmail(session, toEmail, fromEmail, subject, body);
		}
		
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public static void sendEmail(Session session, String toEmail, String fromEmail, String subject, String body) {
		try {
			MimeMessage msg = new MimeMessage(session);
			// set message headers
			msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
			// msg.addHeader("format", "flowed");
			// msg.addHeader("Content-Transfer-Encoding", "8bit");

			msg.setFrom(new InternetAddress(fromEmail, "Pilog"));

			msg.setReplyTo(InternetAddress.parse(fromEmail, false));

			msg.setSubject(subject, "UTF-8");

			msg.setText(body, "UTF-8");

			msg.setSentDate(new Date());

			msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail, false));
			System.out.println("Message is ready");
			Transport.send(msg);

			System.out.println("EMail Sent Successfully!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
