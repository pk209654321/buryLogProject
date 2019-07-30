package bean.pushData;

import java.io.Serializable;

/**
 * @ClassName PushDataResult
 * @Description TODO
 * @Author lenovo
 * @Date 2019/7/29 16:05
 **/
public class PushDataResult implements Serializable {
    private String msg_id;
    private byte[] vtData;
    //PushType
    private Integer ePushDataType;
    private String sBusinessId;
    private String sMsgId;


    private Integer iPushTime;
    private Integer iExpireTime;
    private Integer iStartTime;
    private String sTitle;
    private Integer eDeviceType;
    private String sDescription;
    private Integer iNotifyEffect;


    //PushControlData
    private Integer iRealPushType;
    private String sAndroidIconUrl;
    private String sIosIconUrl;

    private Integer iClassID;
    private String sDisplayType;
    private Integer ePushNotificationType;
    private Integer iBuilderId;
    private Boolean bDefaultCfg;


    public String getMsg_id() {
        return msg_id;
    }

    public void setMsg_id(String msg_id) {
        this.msg_id = msg_id;
    }

    public byte[] getVtData() {
        return vtData;
    }

    public void setVtData(byte[] vtData) {
        this.vtData = vtData;
    }

    public Integer getePushDataType() {
        return ePushDataType;
    }

    public void setePushDataType(Integer ePushDataType) {
        this.ePushDataType = ePushDataType;
    }

    public String getsBusinessId() {
        return sBusinessId;
    }

    public void setsBusinessId(String sBusinessId) {
        this.sBusinessId = sBusinessId;
    }

    public String getsMsgId() {
        return sMsgId;
    }

    public void setsMsgId(String sMsgId) {
        this.sMsgId = sMsgId;
    }

    public Integer getiPushTime() {
        return iPushTime;
    }

    public void setiPushTime(Integer iPushTime) {
        this.iPushTime = iPushTime;
    }

    public Integer getiExpireTime() {
        return iExpireTime;
    }

    public void setiExpireTime(Integer iExpireTime) {
        this.iExpireTime = iExpireTime;
    }

    public Integer getiStartTime() {
        return iStartTime;
    }

    public void setiStartTime(Integer iStartTime) {
        this.iStartTime = iStartTime;
    }

    public String getsTitle() {
        return sTitle;
    }

    public void setsTitle(String sTitle) {
        this.sTitle = sTitle;
    }

    public Integer geteDeviceType() {
        return eDeviceType;
    }

    public void seteDeviceType(Integer eDeviceType) {
        this.eDeviceType = eDeviceType;
    }

    public String getsDescription() {
        return sDescription;
    }

    public void setsDescription(String sDescription) {
        this.sDescription = sDescription;
    }

    public Integer getiNotifyEffect() {
        return iNotifyEffect;
    }

    public void setiNotifyEffect(Integer iNotifyEffect) {
        this.iNotifyEffect = iNotifyEffect;
    }

    public Integer getiRealPushType() {
        return iRealPushType;
    }

    public void setiRealPushType(Integer iRealPushType) {
        this.iRealPushType = iRealPushType;
    }

    public String getsAndroidIconUrl() {
        return sAndroidIconUrl;
    }

    public void setsAndroidIconUrl(String sAndroidIconUrl) {
        this.sAndroidIconUrl = sAndroidIconUrl;
    }

    public String getsIosIconUrl() {
        return sIosIconUrl;
    }

    public void setsIosIconUrl(String sIosIconUrl) {
        this.sIosIconUrl = sIosIconUrl;
    }

    public Integer getiClassID() {
        return iClassID;
    }

    public void setiClassID(Integer iClassID) {
        this.iClassID = iClassID;
    }

    public String getsDisplayType() {
        return sDisplayType;
    }

    public void setsDisplayType(String sDisplayType) {
        this.sDisplayType = sDisplayType;
    }

    public Integer getePushNotificationType() {
        return ePushNotificationType;
    }

    public void setePushNotificationType(Integer ePushNotificationType) {
        this.ePushNotificationType = ePushNotificationType;
    }

    public Integer getiBuilderId() {
        return iBuilderId;
    }

    public void setiBuilderId(Integer iBuilderId) {
        this.iBuilderId = iBuilderId;
    }

    public Boolean getbDefaultCfg() {
        return bDefaultCfg;
    }

    public void setbDefaultCfg(Boolean bDefaultCfg) {
        this.bDefaultCfg = bDefaultCfg;
    }
}
