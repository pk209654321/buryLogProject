package bean.ans;

import java.io.Serializable;

/**
 * @ClassName VQuestionOption
 * @Description TODO
 * @Author lenovo
 * @Date 2019/7/19 15:28
 **/
public class VQuestionOption implements Serializable {
    private int dScore;
    private String sOption;
    private String sTitle;

    public int getdScore() {
        return dScore;
    }

    public void setdScore(int dScore) {
        this.dScore = dScore;
    }

    public String getsOption() {
        return sOption;
    }

    public void setsOption(String sOption) {
        this.sOption = sOption;
    }

    public String getsTitle() {
        return sTitle;
    }

    public void setsTitle(String sTitle) {
        this.sTitle = sTitle;
    }
}
