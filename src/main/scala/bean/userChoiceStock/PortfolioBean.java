package bean.userChoiceStock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName PortfolioBean
 * @Description TODO
 * @Author lenovo
 * @Date 2019/2/14 16:22
 **/
public class PortfolioBean implements Serializable {
    private String sKey;
    private String updateTime;
    private Integer iVersion;
    private Boolean bRecvAnnounce;
    private Boolean bRecvResearch;
    private Float fChipHighPrice;
    private Float fChipLowPrice;
    private Float fDecreasesPer;
    private Float fHighPrice;
    private Float fIncreasePer;
    private Float fLowPrice;
    private Float fMainChipHighPrice;
    private Float fMainChipLowPrice;
    private Integer iCreateTime;
    private Integer iUpdateTime;
    private Boolean sAiAlert;
    private Boolean sDKAlert;
    private Boolean sDel;
    private Boolean sHold;
    private String sDtSecCode;
    private String sName;
    private Integer stCommentInfo_iCreateTime;
    private Integer stCommentInfo_iUpdateTime;
    private String stCommentInfo_sComment;
    private List<Integer> vBroadcastTime;
    private List<Integer> vStrategyId;
    private long lUptTimeExt=0;

    public long getlUptTimeExt() {
        return lUptTimeExt;
    }

    public void setlUptTimeExt(long lUptTimeExt) {
        this.lUptTimeExt = lUptTimeExt;
    }

    public List<Integer> getvBroadcastTime() {
        return vBroadcastTime;
    }

    public void setvBroadcastTime(List<Integer> vBroadcastTime) {
        this.vBroadcastTime = vBroadcastTime;
    }

    public List<Integer> getvStrategyId() {
        return vStrategyId;
    }

    public void setvStrategyId(List<Integer> vStrategyId) {
        this.vStrategyId = vStrategyId;
    }

    public String getsKey() {
        return sKey;
    }

    public void setsKey(String sKey) {
        this.sKey = sKey;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public Boolean getsAiAlert() {
        return sAiAlert;
    }

    public void setsAiAlert(Boolean sAiAlert) {
        this.sAiAlert = sAiAlert;
    }

    public Boolean getsDKAlert() {
        return sDKAlert;
    }

    public void setsDKAlert(Boolean sDKAlert) {
        this.sDKAlert = sDKAlert;
    }

    public Boolean getsDel() {
        return sDel;
    }

    public void setsDel(Boolean sDel) {
        this.sDel = sDel;
    }

    public Boolean getsHold() {
        return sHold;
    }

    public void setsHold(Boolean sHold) {
        this.sHold = sHold;
    }

    public Integer getiVersion() {
        return iVersion;
    }

    public void setiVersion(Integer iVersion) {
        this.iVersion = iVersion;
    }


    public Boolean getbRecvAnnounce() {
        return bRecvAnnounce;
    }

    public void setbRecvAnnounce(Boolean bRecvAnnounce) {
        this.bRecvAnnounce = bRecvAnnounce;
    }

    public Boolean getbRecvResearch() {
        return bRecvResearch;
    }

    public void setbRecvResearch(Boolean bRecvResearch) {
        this.bRecvResearch = bRecvResearch;
    }


    public Float getfChipHighPrice() {
        return fChipHighPrice;
    }

    public void setfChipHighPrice(Float fChipHighPrice) {
        this.fChipHighPrice = fChipHighPrice;
    }

    public Float getfChipLowPrice() {
        return fChipLowPrice;
    }

    public void setfChipLowPrice(Float fChipLowPrice) {
        this.fChipLowPrice = fChipLowPrice;
    }

    public Float getfDecreasesPer() {
        return fDecreasesPer;
    }

    public void setfDecreasesPer(Float fDecreasesPer) {
        this.fDecreasesPer = fDecreasesPer;
    }

    public Float getfHighPrice() {
        return fHighPrice;
    }

    public void setfHighPrice(Float fHighPrice) {
        this.fHighPrice = fHighPrice;
    }

    public Float getfIncreasePer() {
        return fIncreasePer;
    }

    public void setfIncreasePer(Float fIncreasePer) {
        this.fIncreasePer = fIncreasePer;
    }

    public Float getfLowPrice() {
        return fLowPrice;
    }

    public void setfLowPrice(Float fLowPrice) {
        this.fLowPrice = fLowPrice;
    }

    public Float getfMainChipHighPrice() {
        return fMainChipHighPrice;
    }

    public void setfMainChipHighPrice(Float fMainChipHighPrice) {
        this.fMainChipHighPrice = fMainChipHighPrice;
    }

    public Float getfMainChipLowPrice() {
        return fMainChipLowPrice;
    }

    public void setfMainChipLowPrice(Float fMainChipLowPrice) {
        this.fMainChipLowPrice = fMainChipLowPrice;
    }

    public String getsDtSecCode() {
        return sDtSecCode;
    }

    public void setsDtSecCode(String sDtSecCode) {
        this.sDtSecCode = sDtSecCode;
    }

    public String getsName() {
        return sName;
    }

    public void setsName(String sName) {
        this.sName = sName;
    }

    public Integer getStCommentInfo_iCreateTime() {
        return stCommentInfo_iCreateTime;
    }

    public void setStCommentInfo_iCreateTime(Integer stCommentInfo_iCreateTime) {
        this.stCommentInfo_iCreateTime = stCommentInfo_iCreateTime;
    }

    public Integer getStCommentInfo_iUpdateTime() {
        return stCommentInfo_iUpdateTime;
    }

    public void setStCommentInfo_iUpdateTime(Integer stCommentInfo_iUpdateTime) {
        this.stCommentInfo_iUpdateTime = stCommentInfo_iUpdateTime;
    }

    public String getStCommentInfo_sComment() {
        return stCommentInfo_sComment;
    }

    public void setStCommentInfo_sComment(String stCommentInfo_sComment) {
        this.stCommentInfo_sComment = stCommentInfo_sComment;
    }

    public Integer getiCreateTime() {
        return iCreateTime;
    }

    public void setiCreateTime(Integer iCreateTime) {
        this.iCreateTime = iCreateTime;
    }

    public Integer getiUpdateTime() {
        return iUpdateTime;
    }

    public void setiUpdateTime(Integer iUpdateTime) {
        this.iUpdateTime = iUpdateTime;
    }
}
