package test;

import org.apache.commons.lang3.StringUtils;

/**
 * @ClassName TestFunction
 * @Description TODO
 * @Author lenovo
 * @Date 2018/12/4 10:06
 **/
public class TestFunction {
    public static void main(String[] args) {
        String aaa="184.105.139.67";
        String[] aaas = aaa.split("&");
        System.out.println(aaas[0]);//184.105.139.67
        System.out.println(aaas.length);
        String bbb="&184.105.139.67";//1
        String[] bbbs = bbb.split("&");
        System.out.println(StringUtils.isNotBlank(bbbs[0]));//false
        System.out.println(bbbs.length);//2
        String c="a&b&c&192.168.130.161";
        int i = c.lastIndexOf("&");
        String substring = c.substring(0, i);
        String substring1 = c.substring(i+1, c.length());
        System.out.println(substring);//a&b&c
        System.out.println(substring1);//192.168.130.161
    }

    /**
     *
     * @param a
     *
     */
    public static void  test(String a){

    }
}
