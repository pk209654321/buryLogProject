package bean.ans;

import java.io.Serializable;
import java.util.List;

/**
 * @ClassName AnsItem
 * @Description TODO
 * @Author lenovo
 * @Date 2019/7/19 15:24
 **/
public class AnsItem implements Serializable {
    private int dScore;
    private int iMultiOpt;
    private int iQuestionNo;
    private String sAnswer;
    private String sQuestionComment;
    private String sQuestionContent;
    private String sQuestionType;
    private String sQuestionTypeDesc;
    private List<VQuestionOption> vQuestionOption;

    public int getdScore() {
        return dScore;
    }

    public void setdScore(int dScore) {
        this.dScore = dScore;
    }

    public int getiMultiOpt() {
        return iMultiOpt;
    }

    public void setiMultiOpt(int iMultiOpt) {
        this.iMultiOpt = iMultiOpt;
    }

    public int getiQuestionNo() {
        return iQuestionNo;
    }

    public void setiQuestionNo(int iQuestionNo) {
        this.iQuestionNo = iQuestionNo;
    }

    public String getsAnswer() {
        return sAnswer;
    }

    public void setsAnswer(String sAnswer) {
        this.sAnswer = sAnswer;
    }

    public String getsQuestionComment() {
        return sQuestionComment;
    }

    public void setsQuestionComment(String sQuestionComment) {
        this.sQuestionComment = sQuestionComment;
    }

    public String getsQuestionContent() {
        return sQuestionContent;
    }

    public void setsQuestionContent(String sQuestionContent) {
        this.sQuestionContent = sQuestionContent;
    }

    public String getsQuestionType() {
        return sQuestionType;
    }

    public void setsQuestionType(String sQuestionType) {
        this.sQuestionType = sQuestionType;
    }

    public String getsQuestionTypeDesc() {
        return sQuestionTypeDesc;
    }

    public void setsQuestionTypeDesc(String sQuestionTypeDesc) {
        this.sQuestionTypeDesc = sQuestionTypeDesc;
    }

    public List<VQuestionOption> getvQuestionOption() {
        return vQuestionOption;
    }

    public void setvQuestionOption(List<VQuestionOption> vQuestionOption) {
        this.vQuestionOption = vQuestionOption;
    }
}
