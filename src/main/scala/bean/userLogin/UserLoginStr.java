package bean.userLogin;/**
 * Created by lenovo on 2018/10/26.
 */

/**
 * @ClassName UserLoginStr
 * @Description TODO
 * @Author lenovo
 * @Date 2018/10/26 13:52
 **/
public class UserLoginStr {
    private Integer userId;
    private Integer type;
    private Data data;

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }
}
