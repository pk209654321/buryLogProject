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
        String string ="url=https://download.gushi.com/app/download/competitionDown.html?sceneId=1&preview=0|task_id=1003003";
        int i = string.indexOf("=");
        String substring = string.substring(0, i);
        String sub2 = string.substring(i + 1, string.length());
        System.out.println(substring);
        System.out.println(sub2);
    }
}
