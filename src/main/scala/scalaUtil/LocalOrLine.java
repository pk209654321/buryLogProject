package scalaUtil;/**
 * Created by lenovo on 2018/10/30.
 */


import conf.ConfigurationManager;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @ClassName LocalOrLine
 * @Description TODO
 * @Author lenovo
 * @Date 2018/10/30 16:39
 **/
public class LocalOrLine {
    private static String localIp= ConfigurationManager.getProperty("local.ip");


    //判断当前环境是否为本地ip
    public static boolean judgeLocal(){
        try {
            InetAddress addr = InetAddress.getLocalHost();
            String ip = addr.getHostAddress().toString();
            if (ip.equals(localIp)){
                return true;
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return true;
        }
        return false;
    }
}
