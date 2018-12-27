package test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class MainUI {
    
    private static final String REQUEST_PATH = "http://222.240.152.82:10080/burypoint/postBuryPointInfo";
    private static final String BOUNDARY = "20140501";

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        
        URL url = new URL(REQUEST_PATH);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setConnectTimeout(3000); // 设置发起连接的等待时间，3s
        httpConn.setReadTimeout(30000); // 设置数据读取超时的时间，30s
        httpConn.setUseCaches(false); // 设置不使用缓存
        httpConn.setDoOutput(true);
        httpConn.setRequestMethod("POST");
        
        httpConn.setRequestProperty("Connection", "Keep-Alive");
        //httpConn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.1; zh-CN; rv:1.9.2.6)");
        OutputStream os = httpConn.getOutputStream();
        BufferedOutputStream bos = new BufferedOutputStream(os);
        
        String content = "wangyadong";
        bos.write(content.getBytes());
        bos.flush();
        bos.close();
        os.close();
        
         // 读取返回数据  
        StringBuffer strBuf = new StringBuffer();
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                httpConn.getInputStream()));
        String line = null;
        while ((line = reader.readLine()) != null) {
            strBuf.append(line).append("\n");
        }
        String res = strBuf.toString();
        System.out.println(res);
        reader.close();
        httpConn.disconnect();
    }

}