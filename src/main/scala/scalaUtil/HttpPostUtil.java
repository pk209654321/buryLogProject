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
            urlCon.setRequestProperty("Accept-Language", "en-us,en;q=0.5");
            urlCon.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT //5.1)AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.46 Safari/535.11");
            urlCon.setRequestProperty("Content-length",
                    String.valueOf(xmlData.length));
            urlCon.setConnectTimeout(30000);
            urlCon.setReadTimeout(30000);
            DataOutputStream printout = new DataOutputStream(
                    urlCon.getOutputStream());
            printout.write(xmlData);
            printout.flush();
            printout.close();
            instr = urlCon.getInputStream();
            byte[] bis = IOUtils.toByteArray(instr);
            String ResponseString = new String(bis, "UTF-8");
            if ((ResponseString == null) || ("".equals(ResponseString.trim()))) {
                System.out.println("返回空");
            }
            System.out.println("返回数据为:" + ResponseString);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                    instr.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void doHttpPost(String xmlInfo, String urlstr,String key,String value) {
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
            urlCon.setRequestProperty(key, value);
          /*  urlCon.setRequestProperty("content-Type", "application/json");
            urlCon.setRequestProperty("charset", "utf-8");
            urlCon.setRequestProperty("Content-length",
                    String.valueOf(xmlData.length));*/
            urlCon.setConnectTimeout(30000);
            urlCon.setReadTimeout(30000);
           DataOutputStream printout = new DataOutputStream(
                    urlCon.getOutputStream());
            printout.write(xmlData);
            printout.flush();
            printout.close();
            instr = urlCon.getInputStream();
            byte[] bis = IOUtils.toByteArray(instr);
            String ResponseString = new String(bis, "UTF-8");
            if ((ResponseString == null) || ("".equals(ResponseString.trim()))) {
                System.out.println("返回空");
            }
            System.out.println("返回数据为:" + ResponseString);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                instr.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }


    public synchronized static void sendMessage(Object t, String urlstr){
        String toJSONString = JSON.toJSONString(t);
        HttpPostUtil.doHttpPost(toJSONString,urlstr);
    }

    public synchronized static void sendMessageForPolarLight(String urlstr,String key,String value){
        HttpPostUtil.doHttpPost("", urlstr, key, value);
    }

    public static void main(String[] args) {
//        ArrayList<CustomLine> customLines = new ArrayList<>();
//        CustomLine customLine = new CustomLine();
//        customLine.setLast_line_time(1553564233);
//        customLine.setUser_id(95988);
//        CustomLine customLine2 = new CustomLine();
//        customLine2.setLast_line_time(1553578134);
//        customLine2.setUser_id(95991);
//        CustomLine customLine3 = new CustomLine();
//        customLine3.setLast_line_time(1553564233);
//        customLine3.setUser_id(96017);
//        customLines.add(customLine);
//        customLines.add(customLine2);
//        customLines.add(customLine3);
//        StockBean stockBean = new StockBean();
//
//        stockBean.setData(customLines);
//        HttpPostUtil.sendMessage(stockBean,"http://dts.test.gp122.com/api/push-user-online-status");
    }


}
