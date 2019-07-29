package bean.pushData;

/**
 * @ClassName PushDataResult
 * @Description TODO
 * @Author lenovo
 * @Date 2019/7/29 16:05
 **/
public class PushDataResult {
    private Byte[] vtData;

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



}
