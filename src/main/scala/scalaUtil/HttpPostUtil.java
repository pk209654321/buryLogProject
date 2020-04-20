package scalaUtil;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

/**
 * @ClassName HttpPostUtil
 * @Description TODO
 * @Author lenovo
 * @Date 2018/10/26 13:40
 **/
public class HttpPostUtil {
    private static void doHttpPost(String xmlInfo, String urlstr) {
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
            System.out.println("返回数据为:" + ResponseString);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (instr != null) {
                    instr.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private static boolean doHttpPost(String xmlInfo, String urlstr, String key, String value) {
        System.out.println("发起的数据:" + xmlInfo);
        byte[] xmlData = xmlInfo.getBytes();
        InputStream is = null;
        BufferedReader br = null;
        String result;
        HttpURLConnection urlCon = null;
        boolean flag = false;
        try {
            URL url = new URL(urlstr);
            urlCon = (HttpURLConnection) url.openConnection();
            urlCon.setDoOutput(true);
            urlCon.setDoInput(true);
            urlCon.setUseCaches(false);
            urlCon.setRequestProperty(key, value);
            urlCon.setRequestProperty("content-Type", "application/json");
            urlCon.setRequestProperty("charset", "utf-8");
//            urlCon.setRequestProperty("Content-length",
//                    String.valueOf(xmlData.length));
            urlCon.setConnectTimeout(30000);
            urlCon.setReadTimeout(30000);
            DataOutputStream printout = new DataOutputStream(
                    urlCon.getOutputStream());
            printout.write(xmlData);
            printout.flush();
            printout.close();
            if (urlCon.getResponseCode() == 200) { //新返回结果
                is = urlCon.getInputStream();
                // 封装输入流is，并指定字符集
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                // 存放数据
                StringBuilder sbf = new StringBuilder();
                String temp;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
                System.out.println("返回结果==================" + result);
                flag = true;
            }

            /*instr = urlCon.getInputStream();
            byte[] bis = IOUtils.toByteArray(instr);
            String ResponseString = new String(bis, "UTF-8");
            System.out.println("doHttpPost返回数据为============================" + ResponseString);*/
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != br) {
                    br.close();
                }

                if (null != is) {
                    is.close();
                }

                if (null != urlCon) {
                    urlCon.disconnect();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return flag;
    }


    //发送post请求 携带json数据的
    public static void sendPostMethod(String jsonStr, String url, String key, String value) {
        //1.创建httpClient
        CloseableHttpClient httpClient = HttpClients.createDefault();
        //2.创建post请求方式实例
        HttpPost httpPost = new HttpPost(url);

        //2.1设置请求头 发送的是json数据格式
        httpPost.setHeader("Content-type", "application/json;charset=utf-8");
        //httpPost.setHeader("Connection", "Close");
        httpPost.setHeader(key, value);
        //3.设置参数---设置消息实体 也就是携带的数据
        /*
        * 比如传递：
        * {
                "username": "aries",
                "password": "666666"
            }
         */
        //String jsonStr = " {\"username\":\"aries\",\"password\":\"666666\"}";
        StringEntity entity = new StringEntity(jsonStr, Charset.forName("UTF-8"));
        entity.setContentEncoding("UTF-8");  //设置编码格式
        // 发送Json格式的数据请求
        entity.setContentType("application/json");
        //把请求消息实体塞进去
        httpPost.setEntity(entity);

        //4.执行http的post请求
        CloseableHttpResponse httpResponse = null;
        InputStream inputStream = null;
        try {
            httpResponse = httpClient.execute(httpPost);
            //5.对返回的数据进行处理
            //5.1判断是否成功
            System.out.println(httpResponse.getStatusLine().getStatusCode());

            //5.2对数据进行处理
            HttpEntity httpEntity = httpResponse.getEntity();
            inputStream = httpEntity.getContent(); //获取content实体内容
            //封装成字符流来输出
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //6.关闭inputStream和httpResponse
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (httpResponse != null) {
                try {
                    httpResponse.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public synchronized static void sendMessage(Object t, String urlstr) {
        String toJSONString = JSON.toJSONString(t);
        HttpPostUtil.doHttpPost(toJSONString, urlstr);
    }

    public synchronized static void sendMessageForPolarLight(String urlstr, String key, String value) {
        HttpPostUtil.doHttpPost("", urlstr, key, value);
    }

    public static boolean sendMessageForPolarLightNew(String jsonStr, String urlstr, String key, String value) {
        return HttpPostUtil.doHttpPost(jsonStr, urlstr, key, value);
    }

    public static void main(String[] args) {
        String a;
    }


}
