package bean.earlyWarning;

import com.qq.tars.protocol.tars.BaseDecodeStream;
import com.qq.tars.protocol.tars.BaseEncodeStream;
import com.qq.tars.protocol.tars.Message;
import com.qq.tars.protocol.tars.annotation.TarsStruct;
import com.qq.tars.protocol.tars.annotation.TarsStructProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName UserStockAlertCfgData
 * @Description TODO
 * @Author lenovo
 * @Date 2019/4/18 9:52
 **/
@TarsStruct
public class UserStockAlertCfgData extends Message {
    @TarsStructProperty(
            order = 0,
            isRequire = false
    )
    public int iAccountId=-1; // 用户 ID
    @TarsStructProperty(
            order = 1,
            isRequire = false
    )
    public byte[] vGUID=null;// 用户 GUID (预留)
    @TarsStructProperty(
            order = 2,
            isRequire = false
    )
    public StockIntelligentAlert stStockIntelligentAlert;
    @TarsStructProperty(
            order = 3,
            isRequire = false
    )
    public List<StockCustomAlert> vStockCustomAlert;
    @TarsStructProperty(
            order = 4,
            isRequire = false
    )
    public long lUptTime=-1;

    static StockIntelligentAlert cache_stStockIntelligentAlert=new StockIntelligentAlert();
    static List<StockCustomAlert> cash_vStockCustomAlert=new ArrayList<>();
    static {
        StockCustomAlert stockCustomAlert = new StockCustomAlert();
        cash_vStockCustomAlert.add(stockCustomAlert);
    }
    public int getiAccountId() {
        return iAccountId;
    }

    public void setiAccountId(int iAccountId) {
        this.iAccountId = iAccountId;
    }

    public byte[] getvGUID() {
        return vGUID;
    }

    public void setvGUID(byte[] vGUID) {
        this.vGUID = vGUID;
    }

    public StockIntelligentAlert getStStockIntelligentAlert() {
        return stStockIntelligentAlert;
    }

    public void setStStockIntelligentAlert(StockIntelligentAlert stStockIntelligentAlert) {
        this.stStockIntelligentAlert = stStockIntelligentAlert;
    }

    public List<StockCustomAlert> getvStockCustomAlert() {
        return vStockCustomAlert;
    }

    public void setvStockCustomAlert(List<StockCustomAlert> vStockCustomAlert) {
        this.vStockCustomAlert = vStockCustomAlert;
    }

    public long getlUptTime() {
        return lUptTime;
    }

    public void setlUptTime(long lUptTime) {
        this.lUptTime = lUptTime;
    }

    public UserStockAlertCfgData(){

    }

    public UserStockAlertCfgData(int iAccountId, byte[] vGUID, StockIntelligentAlert stStockIntelligentAlert, List<StockCustomAlert> vStockCustomAlert, long lUptTime) {
        this.iAccountId = iAccountId;
        this.vGUID = vGUID;
        this.stStockIntelligentAlert = stStockIntelligentAlert;
        this.vStockCustomAlert = vStockCustomAlert;
        this.lUptTime = lUptTime;
    }


    public void readFrom(BaseDecodeStream is) {
        BaseDecodeStream _is = new BaseDecodeStream(is);
        _is.setCharset(is.getCharset());
        /*this.vProSecInfo = (List)_is.read(0, false, cache_vProSecInfo);
        this.vSubjectInfo = (List)_is.read(1, false, cache_vSubjectInfo);
        this.iVersion = _is.read(2, false, this.iVersion);
        this.vGroupInfo = (List)_is.read(3, false, cache_vGroupInfo);*/

        this.iAccountId=_is.read(0, false,iAccountId);
        this.vGUID=_is.read(1, false, this.vGUID);
        this.stStockIntelligentAlert=(StockIntelligentAlert)_is.read(2, false, cache_stStockIntelligentAlert);
        //this.vStockCustomAlert=(List)_is.read(2, false,cash_vStockCustomAlert);
        this.lUptTime=_is.read(4, false, this.lUptTime);
    }
}
