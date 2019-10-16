package scalaUtil;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @ClassName Md5SumUtil
 * @Description TODO
 * @Author lenovo
 * @Date 2019/10/14 14:25
 **/
public class Md5SumUtil {
    public static String getMd5Sum(String str) {
        byte[] buf = str.getBytes();
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        md5.update(buf);
        byte[] tmp = md5.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : tmp) {
            if (b >= 0 && b < 16)
                sb.append("0");
            sb.append(Integer.toHexString(b & 0xff));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(getMd5Sum("fasdflkjasdflkjllkjjlk1111"));
    }


    /*public void test() throws NoSuchAlgorithmException {
        String str = "123456";
        byte[] buf = str.getBytes();
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(buf);
        byte[] tmp = md5.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : tmp) {
            if (b >= 0 && b < 16)
                sb.append("0");
            sb.append(Integer.toHexString(b & 0xff));
        }
        System.out.println("md5 length:" + tmp.length + " result length:" + sb.toString().length());
        System.out.println(sb);
    }*/

}
