package bean.earlyWarning;

import com.dengtacj.bec.CommentInfo;
import com.qq.tars.protocol.tars.BaseDecodeStream;
import com.qq.tars.protocol.tars.BaseEncodeStream;
import com.qq.tars.protocol.tars.Message;
import com.qq.tars.protocol.tars.annotation.TarsStruct;
import com.qq.tars.protocol.tars.annotation.TarsStructProperty;
import com.qq.tars.protocol.util.TarsUtil;

import java.util.List;

/**
 * @ClassName StockIntelligentAlert
 * @Description TODO
 * @Author lenovo
 * @Date 2019/4/18 9:55
 **/
@TarsStruct
public class StockIntelligentAlert extends Message {
    //        2 optional int iSwitch = 1;                   // 启用开关， 0-关闭， 1-开启
//        3 optional bool bLimitUp = true;                 // 涨停提醒; true-启用， false-停用
//        4 optional bool bLimitDown = true;               // 跌停提醒;
//        5 optional bool bSpeedUp = true;                 // 速拉提醒（火箭发射）
//        6 optional bool bSpeedDown = true;               // 速跌提醒（快速跳水）
//        7 optional bool bChangeAsc = true;      // 涨幅超 5% 提醒
//        8 optional bool bChangeDesc = true;     // 跌幅超 5% 提醒
//        9 optional bool bDay30Highest = true;           // 30 日新高提醒
//        10 optional bool bDay60Highest = true;          // 60 日新高提醒
    @TarsStructProperty(
            order = 1,
            isRequire = false
    )
    private boolean bLimitUp = true;

    @TarsStructProperty(
            order = 2,
            isRequire = false
    )
    private boolean bLimitDown = true;
    @TarsStructProperty(
            order = 3,
            isRequire = false
    )

    private boolean bSpeedUp = true;
    @TarsStructProperty(
            order = 4,
            isRequire = false
    )
    private boolean bSpeedDown = true;
    @TarsStructProperty(
            order = 5,
            isRequire = false
    )
    private boolean bChangeAsc = true;
    @TarsStructProperty(
            order = 6,
            isRequire = false
    )
    private boolean bChangeDesc = true;
    @TarsStructProperty(
            order = 7,
            isRequire = false
    )
    private boolean bDay30Highest = true;
    @TarsStructProperty(
            order = 8,
            isRequire = false
    )
    private boolean bDay60Highest = true;



    public boolean isbLimitUp() {
        return bLimitUp;
    }

    public void setbLimitUp(boolean bLimitUp) {
        this.bLimitUp = bLimitUp;
    }

    public boolean isbLimitDown() {
        return bLimitDown;
    }

    public void setbLimitDown(boolean bLimitDown) {
        this.bLimitDown = bLimitDown;
    }

    public boolean isbSpeedUp() {
        return bSpeedUp;
    }

    public void setbSpeedUp(boolean bSpeedUp) {
        this.bSpeedUp = bSpeedUp;
    }

    public boolean isbSpeedDown() {
        return bSpeedDown;
    }

    public void setbSpeedDown(boolean bSpeedDown) {
        this.bSpeedDown = bSpeedDown;
    }

    public boolean isbChangeAsc() {
        return bChangeAsc;
    }

    public void setbChangeAsc(boolean bChangeAsc) {
        this.bChangeAsc = bChangeAsc;
    }

    public boolean isbChangeDesc() {
        return bChangeDesc;
    }

    public void setbChangeDesc(boolean bChangeDesc) {
        this.bChangeDesc = bChangeDesc;
    }

    public boolean isbDay30Highest() {
        return bDay30Highest;
    }

    public void setbDay30Highest(boolean bDay30Highest) {
        this.bDay30Highest = bDay30Highest;
    }

    public boolean isbDay60Highest() {
        return bDay60Highest;
    }

    public void setbDay60Highest(boolean bDay60Highest) {
        this.bDay60Highest = bDay60Highest;
    }

    public StockIntelligentAlert() {
    }

    public StockIntelligentAlert( boolean bLimitUp, boolean bLimitDown, boolean bSpeedUp, boolean bSpeedDown, boolean bChangeAsc, boolean bChangeDesc, boolean bDay30Highest, boolean bDay60Highest) {
        this.bLimitUp = bLimitUp;
        this.bLimitDown = bLimitDown;
        this.bSpeedUp = bSpeedUp;
        this.bSpeedDown = bSpeedDown;
        this.bChangeAsc = bChangeAsc;
        this.bChangeDesc = bChangeDesc;
        this.bDay30Highest = bDay30Highest;
        this.bDay60Highest = bDay60Highest;
    }


    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (!(obj instanceof StockIntelligentAlert)) {
            return false;
        } else {
            StockIntelligentAlert other = (StockIntelligentAlert)obj;
            return TarsUtil.equals(this.bChangeAsc, other.bChangeAsc)
                    && TarsUtil.equals(this.bChangeDesc, other.bChangeDesc)
                    && TarsUtil.equals(this.bDay30Highest, other.bDay30Highest)
                    && TarsUtil.equals(this.bDay60Highest, other.bDay60Highest)
                    && TarsUtil.equals(this.bLimitDown, other.bLimitDown)
                    && TarsUtil.equals(this.bLimitUp, other.bLimitUp)
                    && TarsUtil.equals(this.bSpeedDown, other.bSpeedDown)
                    && TarsUtil.equals(this.bSpeedUp, other.bSpeedUp);
        }
    }

    public void readFrom(BaseDecodeStream is) {
        BaseDecodeStream _is = new BaseDecodeStream(is);
        _is.setCharset(is.getCharset());
        //this.bLimitUp = _is.read(1, false, this.bLimitUp);
        this.bLimitDown=_is.read(2, false, this.bLimitDown);
        this.bChangeAsc=_is.read(5, false,this.bChangeAsc);
        this.bChangeDesc=_is.read(6, false,this.bChangeDesc);
        this.bDay30Highest=_is.read(7, false, this.bDay30Highest);
        this.bDay60Highest=_is.read(8, false, this.bDay60Highest);
    }

}
