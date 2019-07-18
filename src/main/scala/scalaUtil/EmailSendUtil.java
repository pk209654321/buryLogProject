package scalaUtil;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

import java.util.List;

/**
 *  发送QQ邮件
 *  方式①：发送邮件的简单代码
 *  方式②：发送邮件中添加附件
 *  方式③：发送HTML的邮件格式
 */
public class EmailSendUtil {
 
    public static void testSimpleEmail() throws Exception {
        SimpleEmail email = new SimpleEmail(); //创建一个最简单的email对象
        //这里我使用的是QQ，使用的是smtp服务器，需要身份验证，大家如果是使用其它服务器，可以自己去网上进行搜索
        email.setHostName("smtp.exmail.qq.com");
        //POP3服务器（端口995）
        //SMTP服务器（端口465或587）。
        email.setSmtpPort(465);
        //验证信息(发送的邮箱地址与密码) 注:这里的密码是授权码
        email.setAuthenticator(new DefaultAuthenticator("niufu2019@gp51.com", "L38WkidihGRu3vNN"));
        email.setSSLOnConnect(true); // 是否启用SSL
        email.setFrom("niufu2019@gp51.com"); //发送邮件的地址(和验证信息的地址一样)
        email.setSubject("第一封简单邮件");  //邮件的标题
        email.setMsg("简单的邮件来了哦！！！"); //邮件的内容
        email.addTo("799943474@qq.com"); //发送给哪一个邮件
        email.addTo("2881987989@qq.com");
        email.send();  //进行邮件发送
    }

    public static void doSimpleEmail(String sub,String msg,List<String> sendBys) {

        try {
            SimpleEmail email = new SimpleEmail(); //创建一个最简单的email对象
            //这里我使用的是QQ，使用的是smtp服务器，需要身份验证，大家如果是使用其它服务器，可以自己去网上进行搜索
            email.setHostName("smtp.exmail.qq.com");
            //POP3服务器（端口995）
            //SMTP服务器（端口465或587）。
            email.setSmtpPort(465);
            //验证信息(发送的邮箱地址与密码) 注:这里的密码是授权码
            email.setAuthenticator(new DefaultAuthenticator("niufu2019@gp51.com", "L38WkidihGRu3vNN"));
            email.setSSLOnConnect(true); // 是否启用SSL
            email.setFrom("niufu2019@gp51.com"); //发送邮件的地址(和验证信息的地址一样)
            email.setSubject(sub);  //邮件的标题
            email.setMsg(msg); //邮件的内容
            for (String sendBy : sendBys) {
                email.addTo(sendBy);
            }
            email.send();  //进行邮件发送
        } catch (EmailException e) {
            e.printStackTrace();
        }
    }

    public static void doSendMail(String user, String password, List<String> sendBys,String message) throws Exception {
        SimpleEmail email = new SimpleEmail(); //创建一个最简单的email对象
        //这里我使用的是QQ，使用的是smtp服务器，需要身份验证，大家如果是使用其它服务器，可以自己去网上进行搜索
        email.setHostName("smtp.exmail.qq.com");
        //POP3服务器（端口995）
        //SMTP服务器（端口465或587）。
        email.setSmtpPort(465);
        //验证信息(发送的邮箱地址与密码) 注:这里的密码是授权码
        email.setAuthenticator(new DefaultAuthenticator(user, password));
        email.setSSLOnConnect(true); // 是否启用SSL
        email.setFrom(user); //发送邮件的地址(和验证信息的地址一样)
        email.setSubject("DataLink错误警报");  //邮件的标题
        email.setMsg(message); //邮件的内容
        for (String sendBy : sendBys) {
            email.addTo(sendBy);
        }
        email.send();  //进行邮件发送
    }

    public static void main(String[] args) {
        try {
            testSimpleEmail();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
   /* public void testEmailAttachment() throws Exception {
        EmailAttachment attachment = new EmailAttachment(); //创建一个附件对象
        attachment.setPath("POITestImage/1.jpg");  //放一张项目中的图片(指向真实的附件)
        attachment.setDisposition(EmailAttachment.ATTACHMENT); //完成附件设置
        attachment.setDescription("这张图片是一个..."); //设置附件的描述
        attachment.setName("1.jpg"); //设置附件的名称
        //创建email对象(MultiPartEmail可以操作附件)
        MultiPartEmail email = new MultiPartEmail();
        email.setHostName("smtp.qq.com");
        email.setSmtpPort(465);
        //验证信息(发送的邮箱地址与密码) 注:这里的密码是授权码
        email.setAuthenticator(new DefaultAuthenticator("xxx@qq.com", "填写你的授权码"));
        email.setSSLOnConnect(true); // 是否启用SSL
        email.setFrom("xxx@qq.com"); //发送邮件的地址(和验证信息的地址一样)
        email.addTo("xxx@qq.com");  //发送给哪一个邮件
        email.setSubject("这是一张图片"); //邮件标题
        email.setMsg("我发了一张图片给你看哦！");  //邮件内容
        email.attach(attachment); //把附件加到邮件中
        email.send(); //发送邮件
    }
 */
    /*public void testHtml() throws Exception {
        HtmlEmail email = new HtmlEmail(); // 创建可以写Html的email对象
        email.setHostName("smtp.qq.com");
        email.setSmtpPort(465);
        email.setAuthenticator(new DefaultAuthenticator("xxx@qq.com", "填写你的授权码"));
        email.setSSLOnConnect(true); // 是否启用SSL
        email.setCharset("UTF-8");   //发送的时候如果乱码,配置相应的编码
        email.addTo("xxx@qq.com"); //发送给哪一个邮件
        email.setFrom("xxx@qq.com",  "xx");//xx为发件人名字
        email.setSubject("这里面写HTML，非常厉害");
        //设置HTML的信息
        String url = "https://www.baidu.com/";
        email.setHtmlMsg("<html><h1 style='color:green;'><a href="+url+">www.baidu.com</a></h1>点击进入百度</html>");
        //email.setTextMsg("这个就是很一般的显示"); //也可以配置普通的信息
        email.send();//发送邮件
    }*/
 
}