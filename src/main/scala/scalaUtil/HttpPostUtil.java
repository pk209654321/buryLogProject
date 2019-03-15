package scalaUtil;
/**
 * Created by lenovo on 2018/10/26.
 */
import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * @ClassName HttpPostUtil
 * @Description TODO
 * @Author lenovo
 * @Date 2018/10/26 13:40
 **/
public class HttpPostUtil {
    public static void doHttpPost(String xmlInfo, String urlstr) {
        System.out.println("发起的数据:" + xmlInfo);
        byte[] xmlData = xmlInfo.getBytes();
        InputStream instr = null;
        java.io.ByteArrayOutputStream out = null;
        try {
            //URL url = new URL(URL);
            URL url = new URL(urlstr);
            URLConnection urlCon = url.openConnection();
            urlCon.setDoOutput(true);
            urlCon.setDoInput(true);
            urlCon.setUseCaches(false);
            urlCon.setRequestProperty("content-Type", "application/json");
            urlCon.setRequestProperty("charset", "utf-8");
            urlCon.setRequestProperty("Content-length",
                    String.valueOf(xmlData.length));
            urlCon.setConnectTimeout(30000);
            urlCon.setReadTimeout(30000);
            DataOutputStream printout = new DataOutputStream(
                    urlCon.getOutputStream());
            printout.write(xmlData);
            printout.flush();
            printout.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(out!=null&&instr!=null){
                    out.close();
                    instr.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
    public synchronized static void sendMessage(Object t, String urlstr){
        String toJSONString = JSON.toJSONString(t);
        HttpPostUtil.doHttpPost(toJSONString,urlstr);
    }
}
