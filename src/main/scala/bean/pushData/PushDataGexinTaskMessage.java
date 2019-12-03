package bean.pushData;

import com.niufu.tar.bec.PushControlData;
import com.niufu.tar.bec.PushType;
import com.qq.tars.protocol.tars.annotation.TarsStructProperty;

/**
 * @ClassName PushDataGexinTaskMessage
 * @Description TODO
 * @Author lenovo
 * @Date 2019/11/22 9:41
 **/
public class PushDataGexinTaskMessage {
    public String vtData = null;
    public PushType stPushType = null;
    public int iPushTime = 0;
    public int iExpireTime = 0;
    public int iStartTime = 0;
    public String sTitle = "";
    public int eDeviceType = 0;
    public String sDescription = "";
    public int iNotifyEffect = 0;
    public PushControlData stControl = null;
    public int iClassID = 0;
    public String sDisplayType = "";
    public int ePushNotificationType = 0;
    public int iBuilderId = -1;
    public boolean bDefaultCfg = true;
    public int ePCNotifyType=0;
    public String sPCUrl="";
    public int iComeFrom=0;
    public int ePCDeviceType=0;
    public int ePCClass=0;

    public int getePCNotifyType() {
        return ePCNotifyType;
    }

    public void setePCNotifyType(int ePCNotifyType) {
        this.ePCNotifyType = ePCNotifyType;
    }

    public String getsPCUrl() {
        return sPCUrl;
    }

    public void setsPCUrl(String sPCUrl) {
        this.sPCUrl = sPCUrl;
    }

    public int getiComeFrom() {
        return iComeFrom;
    }

    public void setiComeFrom(int iComeFrom) {
        this.iComeFrom = iComeFrom;
    }

    public int getePCDeviceType() {
        return ePCDeviceType;
    }

    public void setePCDeviceType(int ePCDeviceType) {
        this.ePCDeviceType = ePCDeviceType;
    }

    public int getePCClass() {
        return ePCClass;
    }

    public void setePCClass(int ePCClass) {
        this.ePCClass = ePCClass;
    }

    public String getVtData() {
        return this.vtData;
    }

    public void setVtData(String vtData) {
        this.vtData = vtData;
    }

    public PushType getStPushType() {
        return this.stPushType;
    }

    public void setStPushType(PushType stPushType) {
        this.stPushType = stPushType;
    }

    public int getIPushTime() {
        return this.iPushTime;
    }

    public void setIPushTime(int iPushTime) {
        this.iPushTime = iPushTime;
    }

    public int getIExpireTime() {
        return this.iExpireTime;
    }

    public void setIExpireTime(int iExpireTime) {
        this.iExpireTime = iExpireTime;
    }

    public int getIStartTime() {
        return this.iStartTime;
    }

    public void setIStartTime(int iStartTime) {
        this.iStartTime = iStartTime;
    }

    public String getSTitle() {
        return this.sTitle;
    }

    public void setSTitle(String sTitle) {
        this.sTitle = sTitle;
    }

    public int getEDeviceType() {
        return this.eDeviceType;
    }

    public void setEDeviceType(int eDeviceType) {
        this.eDeviceType = eDeviceType;
    }

    public String getSDescription() {
        return this.sDescription;
    }

    public void setSDescription(String sDescription) {
        this.sDescription = sDescription;
    }

    public int getINotifyEffect() {
        return this.iNotifyEffect;
    }

    public void setINotifyEffect(int iNotifyEffect) {
        this.iNotifyEffect = iNotifyEffect;
    }

    public PushControlData getStControl() {
        return this.stControl;
    }

    public void setStControl(PushControlData stControl) {
        this.stControl = stControl;
    }

    public int getIClassID() {
        return this.iClassID;
    }

    public void setIClassID(int iClassID) {
        this.iClassID = iClassID;
    }

    public String getSDisplayType() {
        return this.sDisplayType;
    }

    public void setSDisplayType(String sDisplayType) {
        this.sDisplayType = sDisplayType;
    }

    public int getEPushNotificationType() {
        return this.ePushNotificationType;
    }

    public void setEPushNotificationType(int ePushNotificationType) {
        this.ePushNotificationType = ePushNotificationType;
    }

    public int getIBuilderId() {
        return this.iBuilderId;
    }

    public void setIBuilderId(int iBuilderId) {
        this.iBuilderId = iBuilderId;
    }

    public boolean getBDefaultCfg() {
        return this.bDefaultCfg;
    }

    public void setBDefaultCfg(boolean bDefaultCfg) {
        this.bDefaultCfg = bDefaultCfg;
    }
}
