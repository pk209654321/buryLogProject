package bean;/**
 * Created by lenovo on 2018/10/26.
 */

import com.alibaba.fastjson.JSON;

/**
 * @ClassName Data
 * @Description TODO
 * @Author lenovo
 * @Date 2018/10/26 13:53
 **/
public class Data {
    private String time_login;

    public String getTime_login() {
        return time_login;
    }

    public void setTime_login(String time_login) {
        this.time_login = time_login;
    }

    public static void main(String[] args) {
        Data data = new Data();
        data.setTime_login("1");
        data.setTime_login("1");
        String s = JSON.toJSONString(data);
        System.out.println(s);
    }
}
