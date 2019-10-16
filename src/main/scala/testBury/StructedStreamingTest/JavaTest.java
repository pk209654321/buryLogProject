package testBury.StructedStreamingTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName JavaTest
 * @Description TODO
 * @Author lenovo
 * @Date 2019/10/11 10:24
 **/
public class JavaTest {
    public static void main(String[] args) {

        for (int i=21;i<=100;i++){
            System.out.println("======================"+i);
            new Thread(new JavaTestRun1(i)).start();
        }
    }
}
