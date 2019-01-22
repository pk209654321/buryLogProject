package testBury;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

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

    public static void main(String[] args) {
        File file = new File("E:\\desk\\日志\\bury.log");
        txt2String(file);
    }


}
