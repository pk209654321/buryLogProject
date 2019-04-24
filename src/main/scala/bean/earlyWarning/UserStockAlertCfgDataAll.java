package bean.earlyWarning;

import com.niufu.tar.bec.StockCustomAlert;
import com.niufu.tar.bec.StockIntelligentAlert;
import com.qq.tars.protocol.tars.annotation.TarsStructProperty;

import java.io.Serializable;
import java.util.List;

/**
 * @ClassName UserStockAlertCfgDataAll
 * @Description TODO
 * @Author lenovo
 * @Date 2019/4/19 19:09
 **/
public class UserStockAlertCfgDataAll implements Serializable {
    public long iAccountId = 0L;
    public String vGUID = null;
    //public StockIntelligentAlert stStockIntelligentAlert = null;
    //public List<StockCustomAlert> vStockCustomAlert = null;
    public long lUptTime = 0L;


    //StockIntelligentAlert
    public int iSwitch = 1;
    public boolean bLimitUp = true;
    public boolean bLimitDown = true;
    public boolean bSpeedUp = true;
    public boolean bSpeedDown = true;
    public boolean bChangeAsc = true;
    public boolean bChangeDesc = true;
    public boolean bDay30Highest = true;
    public boolean bDay60Highest = true;


    //StockCustomAlert
    public String sDtSecCode = "";
    public String sDtSecName = "";
    public double dUpperPoint = -1.0D;
    public double dLowerPoint = -1.0D;
    public double dDayChangeAsc = -1.0D;
    public double dDayChangeDesc = -1.0D;
    public double dFiveMinChangeAsc = -1.0D;
    public double dFiveMinChangeDesc = -1.0D;
    public List<Integer> vBroadcastTime = null;
    public List<Integer> vStrategyId = null;

    public long getiAccountId() {
        return iAccountId;
    }

    public void setiAccountId(long iAccountId) {
        this.iAccountId = iAccountId;
    }

    public String getvGUID() {
        return vGUID;
    }

    public void setvGUID(String vGUID) {
        this.vGUID = vGUID;
    }

    public long getlUptTime() {
        return lUptTime;
    }

    public void setlUptTime(long lUptTime) {
        this.lUptTime = lUptTime;
    }

    public int getiSwitch() {
        return iSwitch;
    }

    public void setiSwitch(int iSwitch) {
        this.iSwitch = iSwitch;
    }

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

    public UserStockAlertCfgDataAll() {
    }



    public UserStockAlertCfgDataAll(long iAccountId, String vGUID, long lUptTime, int iSwitch, boolean bLimitUp, boolean bLimitDown, boolean bSpeedUp, boolean bSpeedDown, boolean bChangeAsc, boolean bChangeDesc, boolean bDay30Highest, boolean bDay60Highest, String sDtSecCode, String sDtSecName, double dUpperPoint, double dLowerPoint, double dDayChangeAsc, double dDayChangeDesc, double dFiveMinChangeAsc, double dFiveMinChangeDesc, List<Integer> vBroadcastTime, List<Integer> vStrategyId) {
        this.iAccountId = iAccountId;
        this.vGUID = vGUID;
        this.lUptTime = lUptTime;
        this.iSwitch = iSwitch;
        this.bLimitUp = bLimitUp;
        this.bLimitDown = bLimitDown;
        this.bSpeedUp = bSpeedUp;
        this.bSpeedDown = bSpeedDown;
        this.bChangeAsc = bChangeAsc;
        this.bChangeDesc = bChangeDesc;
        this.bDay30Highest = bDay30Highest;
        this.bDay60Highest = bDay60Highest;
        this.sDtSecCode = sDtSecCode;
        this.sDtSecName = sDtSecName;
        this.dUpperPoint = dUpperPoint;
        this.dLowerPoint = dLowerPoint;
        this.dDayChangeAsc = dDayChangeAsc;
        this.dDayChangeDesc = dDayChangeDesc;
        this.dFiveMinChangeAsc = dFiveMinChangeAsc;
        this.dFiveMinChangeDesc = dFiveMinChangeDesc;
        this.vBroadcastTime = vBroadcastTime;
        this.vStrategyId = vStrategyId;
    }
}
