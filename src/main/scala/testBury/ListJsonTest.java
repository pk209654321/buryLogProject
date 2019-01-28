package testBury;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import testBury.beanTest.BuryTestBean;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName ListJsonTest
 * @Description TODO
 * @Author lenovo
 * @Date 2019/1/28 10:39
 **/
public class ListJsonTest {
    public static void main(String[] args) throws Exception{
        FileInputStream fileInputStream = new FileInputStream("E:\\desk\\文档\\bury.1548518401887");
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String temp=null;
        int flag=0;
        List<BuryTestBean> list=new ArrayList<BuryTestBean>();
        while ((temp=bufferedReader.readLine())!=null){
            String s = temp.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=");
            int i = s.lastIndexOf("&");
            String json = s.substring(0, i);
            json="["+json+"]";
            List<BuryTestBean> buryTestBeans = JSON.parseArray(json, BuryTestBean.class);
            flag++;
            list.addAll(buryTestBeans);
            if (flag>=20){
                break;
            }
        }
        bufferedReader.close();
//        String string = JSON.toJSONString(list);
//        System.out.println(string);
        for (BuryTestBean buryTestBean : list) {
            String line = buryTestBean.getLine();
            System.out.println(line);
        }

    }
}
