//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package bean.selectStock;

import com.niufu.tar.bec.CommentInfo;
import com.qq.tars.protocol.tars.BaseDecodeStream;
import com.qq.tars.protocol.tars.BaseEncodeStream;
import com.qq.tars.protocol.tars.Message;
import com.qq.tars.protocol.tars.annotation.TarsStruct;
import com.qq.tars.protocol.tars.annotation.TarsStructProperty;
import com.qq.tars.protocol.util.TarsUtil;
import java.util.ArrayList;
import java.util.List;

@TarsStruct
public class ProSecInfo extends Message {
    @TarsStructProperty(
        order = 0,
        isRequire = false
    )
    public String sDtSecCode = "";
    @TarsStructProperty(
        order = 1,
        isRequire = false
    )
    public float fHighPrice = -1.0F;
    @TarsStructProperty(
        order = 2,
        isRequire = false
    )
    public float fLowPrice = -1.0F;
    @TarsStructProperty(
        order = 3,
        isRequire = false
    )
    public float fIncreasePer = -1.0F;
    @TarsStructProperty(
        order = 4,
        isRequire = false
    )
    public float fDecreasesPer = -1.0F;
    @TarsStructProperty(
        order = 5,
        isRequire = false
    )
    public boolean bRecvAnnounce = false;
    @TarsStructProperty(
        order = 6,
        isRequire = false
    )
    public boolean bRecvResearch = false;
    @TarsStructProperty(
        order = 7,
        isRequire = false
    )
    public boolean isDel = false;
    @TarsStructProperty(
        order = 8,
        isRequire = false
    )
    public int iCreateTime = -1;
    @TarsStructProperty(
        order = 9,
        isRequire = false
    )
    public int iUpdateTime = -1;
    @TarsStructProperty(
        order = 10,
        isRequire = false
    )
    public String sName = "";
    @TarsStructProperty(
        order = 11,
        isRequire = false
    )
    public boolean isHold = false;
    @TarsStructProperty(
        order = 12,
        isRequire = false
    )
    public CommentInfo stCommentInfo = null;
    @TarsStructProperty(
        order = 13,
        isRequire = false
    )
    public boolean isAiAlert = true;
    @TarsStructProperty(
        order = 14,
        isRequire = false
    )
    public boolean isDKAlert = false;
    @TarsStructProperty(
        order = 15,
        isRequire = false
    )
    public List<Integer> vBroadcastTime = null;
    @TarsStructProperty(
        order = 16,
        isRequire = false
    )
    public float fChipHighPrice = -1.0F;
    @TarsStructProperty(
        order = 17,
        isRequire = false
    )
    public float fChipLowPrice = -1.0F;
    @TarsStructProperty(
        order = 18,
        isRequire = false
    )
    public float fMainChipHighPrice = -1.0F;
    @TarsStructProperty(
        order = 19,
        isRequire = false
    )
    public float fMainChipLowPrice = -1.0F;
    @TarsStructProperty(
        order = 20,
        isRequire = false
    )
    public List<Integer> vStrategyId = null;
    @TarsStructProperty(
        order = 22,
        isRequire = false
    )
    public long lUptTimeExt = 0L;
    static CommentInfo cache_stCommentInfo = new CommentInfo();
    static List<Integer> cache_vBroadcastTime = new ArrayList();
    static List<Integer> cache_vStrategyId;

    public String getSDtSecCode() {
        return this.sDtSecCode;
    }

    public void setSDtSecCode(String sDtSecCode) {
        this.sDtSecCode = sDtSecCode;
    }

    public float getFHighPrice() {
        return this.fHighPrice;
    }

    public void setFHighPrice(float fHighPrice) {
        this.fHighPrice = fHighPrice;
    }

    public float getFLowPrice() {
        return this.fLowPrice;
    }

    public void setFLowPrice(float fLowPrice) {
        this.fLowPrice = fLowPrice;
    }

    public float getFIncreasePer() {
        return this.fIncreasePer;
    }

    public void setFIncreasePer(float fIncreasePer) {
        this.fIncreasePer = fIncreasePer;
    }

    public float getFDecreasesPer() {
        return this.fDecreasesPer;
    }

    public void setFDecreasesPer(float fDecreasesPer) {
        this.fDecreasesPer = fDecreasesPer;
    }

    public boolean getBRecvAnnounce() {
        return this.bRecvAnnounce;
    }

    public void setBRecvAnnounce(boolean bRecvAnnounce) {
        this.bRecvAnnounce = bRecvAnnounce;
    }

    public boolean getBRecvResearch() {
        return this.bRecvResearch;
    }

    public void setBRecvResearch(boolean bRecvResearch) {
        this.bRecvResearch = bRecvResearch;
    }

    public boolean isDel() {
        return this.isDel;
    }

    public void setDel(boolean isDel) {
        this.isDel = isDel;
    }

    public int getICreateTime() {
        return this.iCreateTime;
    }

    public void setICreateTime(int iCreateTime) {
        this.iCreateTime = iCreateTime;
    }

    public int getIUpdateTime() {
        return this.iUpdateTime;
    }

    public void setIUpdateTime(int iUpdateTime) {
        this.iUpdateTime = iUpdateTime;
    }

    public String getSName() {
        return this.sName;
    }

    public void setSName(String sName) {
        this.sName = sName;
    }

    public boolean isHold() {
        return this.isHold;
    }

    public void setHold(boolean isHold) {
        this.isHold = isHold;
    }

    public CommentInfo getStCommentInfo() {
        return this.stCommentInfo;
    }

    public void setStCommentInfo(CommentInfo stCommentInfo) {
        this.stCommentInfo = stCommentInfo;
    }

    public boolean isAiAlert() {
        return this.isAiAlert;
    }

    public void setAiAlert(boolean isAiAlert) {
        this.isAiAlert = isAiAlert;
    }

    public boolean isDKAlert() {
        return this.isDKAlert;
    }

    public void setDKAlert(boolean isDKAlert) {
        this.isDKAlert = isDKAlert;
    }

    public List<Integer> getVBroadcastTime() {
        return this.vBroadcastTime;
    }

    public void setVBroadcastTime(List<Integer> vBroadcastTime) {
        this.vBroadcastTime = vBroadcastTime;
    }

    public float getFChipHighPrice() {
        return this.fChipHighPrice;
    }

    public void setFChipHighPrice(float fChipHighPrice) {
        this.fChipHighPrice = fChipHighPrice;
    }

    public float getFChipLowPrice() {
        return this.fChipLowPrice;
    }

    public void setFChipLowPrice(float fChipLowPrice) {
        this.fChipLowPrice = fChipLowPrice;
    }

    public float getFMainChipHighPrice() {
        return this.fMainChipHighPrice;
    }

    public void setFMainChipHighPrice(float fMainChipHighPrice) {
        this.fMainChipHighPrice = fMainChipHighPrice;
    }

    public float getFMainChipLowPrice() {
        return this.fMainChipLowPrice;
    }

    public void setFMainChipLowPrice(float fMainChipLowPrice) {
        this.fMainChipLowPrice = fMainChipLowPrice;
    }

    public List<Integer> getVStrategyId() {
        return this.vStrategyId;
    }

    public void setVStrategyId(List<Integer> vStrategyId) {
        this.vStrategyId = vStrategyId;
    }

    public long getLUptTimeExt() {
        return this.lUptTimeExt;
    }

    public void setLUptTimeExt(long lUptTimeExt) {
        this.lUptTimeExt = lUptTimeExt;
    }

    public ProSecInfo() {
    }

    public ProSecInfo(String sDtSecCode, float fHighPrice, float fLowPrice, float fIncreasePer, float fDecreasesPer, boolean bRecvAnnounce, boolean bRecvResearch, boolean isDel, int iCreateTime, int iUpdateTime, String sName, boolean isHold, CommentInfo stCommentInfo, boolean isAiAlert, boolean isDKAlert, List<Integer> vBroadcastTime, float fChipHighPrice, float fChipLowPrice, float fMainChipHighPrice, float fMainChipLowPrice, List<Integer> vStrategyId, long lUptTimeExt) {
        this.sDtSecCode = sDtSecCode;
        this.fHighPrice = fHighPrice;
        this.fLowPrice = fLowPrice;
        this.fIncreasePer = fIncreasePer;
        this.fDecreasesPer = fDecreasesPer;
        this.bRecvAnnounce = bRecvAnnounce;
        this.bRecvResearch = bRecvResearch;
        this.isDel = isDel;
        this.iCreateTime = iCreateTime;
        this.iUpdateTime = iUpdateTime;
        this.sName = sName;
        this.isHold = isHold;
        this.stCommentInfo = stCommentInfo;
        this.isAiAlert = isAiAlert;
        this.isDKAlert = isDKAlert;
        this.vBroadcastTime = vBroadcastTime;
        this.fChipHighPrice = fChipHighPrice;
        this.fChipLowPrice = fChipLowPrice;
        this.fMainChipHighPrice = fMainChipHighPrice;
        this.fMainChipLowPrice = fMainChipLowPrice;
        this.vStrategyId = vStrategyId;
        this.lUptTimeExt = lUptTimeExt;
    }



    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (!(obj instanceof ProSecInfo)) {
            return false;
        } else {
            ProSecInfo other = (ProSecInfo)obj;
            return TarsUtil.equals(this.sDtSecCode, other.sDtSecCode) && TarsUtil.equals(this.fHighPrice, other.fHighPrice) && TarsUtil.equals(this.fLowPrice, other.fLowPrice) && TarsUtil.equals(this.fIncreasePer, other.fIncreasePer) && TarsUtil.equals(this.fDecreasesPer, other.fDecreasesPer) && TarsUtil.equals(this.bRecvAnnounce, other.bRecvAnnounce) && TarsUtil.equals(this.bRecvResearch, other.bRecvResearch) && TarsUtil.equals(this.isDel, other.isDel) && TarsUtil.equals(this.iCreateTime, other.iCreateTime) && TarsUtil.equals(this.iUpdateTime, other.iUpdateTime) && TarsUtil.equals(this.sName, other.sName) && TarsUtil.equals(this.isHold, other.isHold) && TarsUtil.equals(this.stCommentInfo, other.stCommentInfo) && TarsUtil.equals(this.isAiAlert, other.isAiAlert) && TarsUtil.equals(this.isDKAlert, other.isDKAlert) && TarsUtil.equals(this.vBroadcastTime, other.vBroadcastTime) && TarsUtil.equals(this.fChipHighPrice, other.fChipHighPrice) && TarsUtil.equals(this.fChipLowPrice, other.fChipLowPrice) && TarsUtil.equals(this.fMainChipHighPrice, other.fMainChipHighPrice) && TarsUtil.equals(this.fMainChipLowPrice, other.fMainChipLowPrice) && TarsUtil.equals(this.vStrategyId, other.vStrategyId) && TarsUtil.equals(this.lUptTimeExt, other.lUptTimeExt);
        }
    }

    public void writeTo(BaseEncodeStream os) {
        BaseEncodeStream _os = new BaseEncodeStream(os);
        _os.setCharset(os.getCharset());
        if (null != this.sDtSecCode) {
            _os.write(0, this.sDtSecCode);
        }

        _os.write(1, this.fHighPrice);
        _os.write(2, this.fLowPrice);
        _os.write(3, this.fIncreasePer);
        _os.write(4, this.fDecreasesPer);
        _os.write(5, this.bRecvAnnounce);
        _os.write(6, this.bRecvResearch);
        _os.write(7, this.isDel);
        _os.write(8, this.iCreateTime);
        _os.write(9, this.iUpdateTime);
        if (null != this.sName) {
            _os.write(10, this.sName);
        }

        _os.write(11, this.isHold);
        if (null != this.stCommentInfo) {
            _os.write(12, this.stCommentInfo);
        }

        _os.write(13, this.isAiAlert);
        _os.write(14, this.isDKAlert);
        if (null != this.vBroadcastTime) {
            _os.write(15, this.vBroadcastTime);
        }

        _os.write(16, this.fChipHighPrice);
        _os.write(17, this.fChipLowPrice);
        _os.write(18, this.fMainChipHighPrice);
        _os.write(19, this.fMainChipLowPrice);
        if (null != this.vStrategyId) {
            _os.write(20, this.vStrategyId);
        }

        _os.write(22, this.lUptTimeExt);
    }

    public void readFrom(BaseDecodeStream is) {
        BaseDecodeStream _is = new BaseDecodeStream(is);
        _is.setCharset(is.getCharset());
        this.sDtSecCode = _is.readString(0, false);
        this.fHighPrice = _is.read(1, false, this.fHighPrice);
        this.fLowPrice = _is.read(2, false, this.fLowPrice);
        this.fIncreasePer = _is.read(3, false, this.fIncreasePer);
        this.fDecreasesPer = _is.read(4, false, this.fDecreasesPer);
        this.bRecvAnnounce = _is.read(5, false, this.bRecvAnnounce);
        this.bRecvResearch = _is.read(6, false, this.bRecvResearch);
        this.isDel = _is.read(7, false, this.isDel);
        this.iCreateTime = _is.read(8, false, this.iCreateTime);
        this.iUpdateTime = _is.read(9, false, this.iUpdateTime);
        this.sName = _is.readString(10, false);
        this.isHold = _is.read(11, false, this.isHold);
        this.stCommentInfo = (CommentInfo)_is.read(12, false, cache_stCommentInfo);
        this.isAiAlert = _is.read(13, false, this.isAiAlert);
        this.isDKAlert = _is.read(14, false, this.isDKAlert);
        this.vBroadcastTime = (List)_is.read(15, false, cache_vBroadcastTime);
        this.fChipHighPrice = _is.read(16, false, this.fChipHighPrice);
        this.fChipLowPrice = _is.read(17, false, this.fChipLowPrice);
        this.fMainChipHighPrice = _is.read(18, false, this.fMainChipHighPrice);
        this.fMainChipLowPrice = _is.read(19, false, this.fMainChipLowPrice);
        this.vStrategyId = (List)_is.read(20, false, cache_vStrategyId);
        this.lUptTimeExt = _is.read(22, false, this.lUptTimeExt);
    }

    static {
        int var_420 = 0;
        cache_vBroadcastTime.add(Integer.valueOf(var_420));
        cache_vStrategyId = new ArrayList();
        var_420 = 0;
        cache_vStrategyId.add(Integer.valueOf(var_420));
    }
}
