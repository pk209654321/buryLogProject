package testBury.StructedStreamingTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName JavaTestRun1
 * @Description TODO
 * @Author lenovo
 * @Date 2019/10/11 10:42
 **/
public class JavaTestRun1 implements Runnable{
    private int i;
    public JavaTestRun1(int i){
        this.i=i;
    }
    @Override
    public void run() {
        Connection connection = DruidUtils.getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("insert into user_test(name,create_time)values ("+i+",current_timestamp)");
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
