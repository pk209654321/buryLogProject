package testBury.javaTest;

/**
 * @ClassName Test2
 * @Description TODO
 * @Author lenovo
 * @Date 2019/7/22 9:44
 **/
public class Test2 {
    public static void main(String[] args) {
        String sub="123";
        int[] intarry=new int[sub.length()];
        for(int i=0;i<sub.length();i++)
        {
            intarry[i]=Integer.parseInt(String.valueOf(sub.charAt(i)));
        }
        for(int i=0;i<intarry.length;i++)
        {
            System.out.println(intarry[i]);
        }
    }
}
