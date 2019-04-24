package testBury.javaTest;

/**
 * @ClassName TempPost
 * @Description TODO
 * @Author lenovo
 * @Date 2019/1/16 16:59
 **/


import com.alibaba.fastjson.JSONObject;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import java.util.UUID;
import java.nio.charset.Charset;
public class TempPost {
    public static boolean httpPostWithJson(String jsonObj, String url){
        boolean isSuccess = false;

        HttpPost post = null;
        try {
            HttpClient httpClient = new DefaultHttpClient();
            post = new HttpPost(url);
            // 构造消息头
            post.setHeader("Content-type", "application/json; charset=utf-8");
            // 构建消息实体
            StringEntity entity = new StringEntity(jsonObj.toString(), Charset.forName("UTF-8"));
            entity.setContentEncoding("UTF-8");
            // 发送Json格式的数据请求
            entity.setContentType("application/json");
            post.setEntity(entity);

            HttpResponse response = httpClient.execute(post);

        } catch (Exception e) {
            e.printStackTrace();
            isSuccess = false;
        }finally{
            if(post != null){
                try {
                    post.releaseConnection();
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return isSuccess;
    }


    // 构建唯一会话Id
    public static String getSessionId(){
        UUID uuid = UUID.randomUUID();
        String str = uuid.toString();
        return str.substring(0, 8) + str.substring(9, 13) + str.substring(14, 18) + str.substring(19, 23) + str.substring(24);
    }

    public static void main(String[] args) {

    }
}
