package bean.crmUserInfo;

/**
 * @ClassName CustomActivityLevel
 * @Description TODO
 * @Author lenovo
 * @Date 2019/3/25 15:34
 **/
public class CustomActivityLevel {
    private Integer user_id;
    private Integer score;
    private String frequency;

    public Integer getUser_id() {
        return user_id;
    }

    public void setUser_id(Integer user_id) {
        this.user_id = user_id;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public String getFrequency() {
        return frequency;
    }

    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }
}
