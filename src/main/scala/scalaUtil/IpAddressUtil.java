package scalaUtil;

import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

public class IpAddressUtil {
    public String sendMessage(String strReqJsonStr, String urlValue) {
        StringBuffer result = new StringBuffer();
        try {
            URL url = new URL(urlValue);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "text/xml; charset=UTF-8");
            con.connect();

            OutputStream outputStream = con.getOutputStream();
            StringBuilder sb = new StringBuilder("xmldata=");
            sb.append("dom.asXML()");
            //参数
            outputStream.write(sb.toString().getBytes());
            outputStream.flush();
            //log.info("HTTP状态码={}", con.getResponseCode());
            BufferedReader inn = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
            String value = inn.readLine();
            while (value != null) {
                result.append(value);
                value = inn.readLine();
            }

            inn.close();
            outputStream.close();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result.toString();
    }

    public static String getMessage(String urlValue) {
        StringBuffer result = new StringBuffer();
        BufferedReader inn = null;
        try {
            URL url = new URL(urlValue);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setRequestMethod("GET");
            con.setRequestProperty("Content-Type", "text/xml; charset=UTF-8");
            con.connect();
            //log.info("HTTP状态码={}", con.getResponseCode());
            inn = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
            String value = inn.readLine().trim();
            while (value != null) {
                if (!"".equals(value)) {
                    result.append(value.trim() + "\n");
                }
                value = inn.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != inn) {
                try {
                    inn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return result.toString();
    }

    public static String getAddress(String ip) throws Exception{
        StringBuilder sb = new StringBuilder();
        String message = getMessage("http://api.online-service.vip/ip3?ip=" + ip);
        JSONObject jsonObject = JSONObject.parseObject(message);
        JSONObject data = jsonObject.getJSONObject("data");
        String country = data.getString("country");
        String province = data.getString("province");
        String city = data.getString("city");
        if("0".equals(country)){
            country=null;
        }
        if("0".equals(province)){
            province=null;
        }
        if("0".equals(city)){
            city=null;
        }
        sb.append(country).append(",").append(province).append(",").append(city);
        return sb.toString();
    }

    public void test() {
        getMessage("http://api.online-service.vip/ip?ip=218.75.123.42");
    }

    public static void main(String[] args) {

        /*long time = new Date().getTime();
        for (int i = 0; i < 10000; i++) {
            String message = IpAddressUtil.getMessage(" http://api.online-service.vip/ip3?ip=218.75.123.42");
            //System.out.println(message);
        }
        long time1 = new Date().getTime();
        long l = (time - time1) / 1000;
        System.out.println(l);*/

        //String address = getAddress("218.75.123.42");
       // System.out.println(address);

    }
}



