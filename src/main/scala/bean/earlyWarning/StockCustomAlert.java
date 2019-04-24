package bean.earlyWarning;

import com.qq.tars.protocol.tars.Message;
import com.qq.tars.protocol.tars.annotation.TarsStructProperty;

import java.util.List;

/**
 * @ClassName StockCustomAlert
 * @Description TODO
 * @Author lenovo
 * @Date 2019/4/18 10:03
 **/
public class StockCustomAlert extends Message {
    //     0 optional string sDtSecCode; // 股票代码或者指数代码
//        1 optional string sDtSecName; // 股票名称
//        2 optional double dUpperPoint = -1.0; // 上涨到指定价格(如果是指数，则是上涨到指定点数)
//        3 optional double dLowerPoint = -1.0; // 下跌到指定价格(如果是指数，则是下跌到指定点数)
//        4 optional double dDayChangeAsc = -1.0; // 日涨幅超过指定点推送
//        5 optional double dDayChangeDesc = -1.0; // 日跌幅超过指定点推送
//        6 optional double dFiveMinChangeAsc = -1.0; // 5 分钟涨幅超过 xxx
//        7 optional double dFiveMinChangeDesc = -1.0; // 5 分钟跌幅超过 xxx
//    // 8 optional StockAlertComment stStockAlertComment; // 备注
//        9 optional vector<int> vBroadcastTime;	//盘中播报时间点列表: 根据以前的配置来
//       10 optional vector<int> vStrategyId;	//设置策略提醒列表
//    // 11 optional bool bIsDel = false; // 是否删除
    @TarsStructProperty(
            order = 0,
            isRequire = false
    )
    private String sDtSecCode = "";
    @TarsStructProperty(
            order = 1,
            isRequire = false
    )
    private String sDtSecName = "";
    @TarsStructProperty(
            order = 2,
            isRequire = false
    )
    private double dUpperPoint = -1.0;
    @TarsStructProperty(
            order = 3,
            isRequire = false
    )
    private double dLowerPoint = -1.0;
    @TarsStructProperty(
            order = 4,
            isRequire = false
    )
    private double dDayChangeAsc = -1.0;
    @TarsStructProperty(
            order = 5,
            isRequire = false
    )
    private double dDayChangeDesc = -1.0;
    @TarsStructProperty(
            order = 6,
            isRequire = false
    )
    private double dFiveMinChangeAsc = -1.0;
    @TarsStructProperty(
            order = 7,
            isRequire = false
    )
    private double dFiveMinChangeDesc = -1.0;
    @TarsStructProperty(
            order = 9,
            isRequire = false
    )
    private List<Integer> vBroadcastTime = null;
    @TarsStructProperty(
            order = 10,
            isRequire = false
    )
    private List<Integer> vStrategyId = null;

    public String getsDtSecCode() {
        return sDtSecCode;
    }

    public void setsDtSecCode(String sDtSecCode) {
        this.sDtSecCode = sDtSecCode;
    }

    public String getsDtSecName() {
        return sDtSecName;
    }

    public void setsDtSecName(String sDtSecName) {
        this.sDtSecName = sDtSecName;
    }

    public double getdUpperPoint() {
        return dUpperPoint;
    }

    public void setdUpperPoint(double dUpperPoint) {
        this.dUpperPoint = dUpperPoint;
    }

    public double getdLowerPoint() {
        return dLowerPoint;
    }

    public void setdLowerPoint(double dLowerPoint) {
        this.dLowerPoint = dLowerPoint;
    }

    public double getdDayChangeAsc() {
        return dDayChangeAsc;
    }

    public void setdDayChangeAsc(double dDayChangeAsc) {
        this.dDayChangeAsc = dDayChangeAsc;
    }

    public double getdDayChangeDesc() {
        return dDayChangeDesc;
    }

    public void setdDayChangeDesc(double dDayChangeDesc) {
        this.dDayChangeDesc = dDayChangeDesc;
    }

    public double getdFiveMinChangeAsc() {
        return dFiveMinChangeAsc;
    }

    public void setdFiveMinChangeAsc(double dFiveMinChangeAsc) {
        this.dFiveMinChangeAsc = dFiveMinChangeAsc;
    }

    public double getdFiveMinChangeDesc() {
        return dFiveMinChangeDesc;
    }

    public void setdFiveMinChangeDesc(double dFiveMinChangeDesc) {
        this.dFiveMinChangeDesc = dFiveMinChangeDesc;
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
}
