package scalaUtil;/**
 * @Auther: lenovo
 * @Date: 2018/12/24 16:31
 * @Description:
 */

import conf.ConfigurationManager;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * @ClassName MailUtil
 * @Description TODO
 * @Author lenovo
 * @Date 2018/12/24 16:31
 **/
public class MailUtil {
    private static final String EMAIL_SEND_USER = ConfigurationManager.getProperty("email.send.user");
    private static final String EMAIL_SEND_PASSWORD = ConfigurationManager.getProperty("email.send.password");
    private static final String EMAIL_SEND_USER1 = ConfigurationManager.getProperty("email.receive.user1");
    private static final String EMAIL_SEND_USER2 = ConfigurationManager.getProperty("email.receive.user2");
    private static final String EMAIL_SEND_USER3 = ConfigurationManager.getProperty("email.receive.user3");

    public static void sendMail(String title,String content) {
        try {
            Properties properties = new Properties();
            properties.put("mail.transport.protocol", "smtp");// 连接协议
            properties.put("mail.smtp.host", "smtp.qq.com");// 主机名
            properties.put("mail.smtp.port", 465);// 端口号
            properties.put("mail.smtp.auth", "true");
            properties.put("mail.smtp.ssl.enable", "true");// 设置是否使用ssl安全连接 ---一般都使用
            properties.put("mail.debug", "true");// 设置是否显示debug信息 true 会在控制台显示相关信息
            // 得到回话对象
            Session session = Session.getInstance(properties);
            // 获取邮件对象
            Message message = new MimeMessage(session);
            // 设置发件人邮箱地址
            message.setFrom(new InternetAddress(EMAIL_SEND_USER));
            // 设置收件人邮箱地址
            message.setRecipients(Message.RecipientType.TO, new InternetAddress[]{new InternetAddress(EMAIL_SEND_USER1),new InternetAddress(EMAIL_SEND_USER2),new InternetAddress(EMAIL_SEND_USER3)});
            //message.setRecipient(Message.RecipientType.TO, new InternetAddress("xxx@qq.com"));//一个收件人
            // 设置邮件标题
            message.setSubject(title);
            // 设置邮件内容
            message.setText(content);
            // 得到邮差对象
            Transport transport = session.getTransport();
            // 连接自己的邮箱账户
            transport.connect(EMAIL_SEND_USER, EMAIL_SEND_PASSWORD);// 密码为QQ邮箱开通的stmp服务后得到的客户端授权码
            // 发送邮件
            transport.sendMessage(message, message.getAllRecipients());
            transport.close();
        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
            sendMail("ceshi","请忽略");
    }

}
