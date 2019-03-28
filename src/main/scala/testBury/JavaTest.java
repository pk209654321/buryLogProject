package testBury;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

/**
 * @ClassName JavaTest
 * @Description TODO
 * @Author lenovo
 * @Date 2019/1/15 19:47
 **/
public class JavaTest {
    public static String txt2String(File file){
        StringBuilder result = new StringBuilder();
        try{
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                HttpURLConnectionPost.readContentFromPost(s,"http://188.185.1.52/burypoint/postBuryPointInfo");
                //String s1 = s.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=");
                //TempPost.httpPostWithJson(s1, "http://188.185.1.52/burypoint/postBuryPointInfo");
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return result.toString();
    }

    /*public static void main(String[] args) {
        File file = new File("E:\\desk\\日志\\bury.log");
        txt2String(file);
    }*/

    /*public static void main(String[] args) throws Exception {
        RpcForUserLineClient rpcClient = new RpcForUserLineClient();
        rpcClient.call("wyd测试");
    }*/

    public static void main(String[] args) throws Exception {
     /*   {

	“api” : “”, //api

	“version” : 1,//版本号

	“timestamp” : xxxx, //  毫秒时间戳，整数类型，即最后三位为毫秒

	“data” : {}, // 数据为空时只能为空对象

        }*/

        ArrayList<String> strings = new ArrayList<>();
        strings.add("_rd");
        strings.add("_ts");
        strings.add("user_id");
        strings.add("last_line_time");
        //strings.add("data");
        Collections.sort(strings);
        for (String string:strings){
            System.out.println(string);
        }
    }


}
