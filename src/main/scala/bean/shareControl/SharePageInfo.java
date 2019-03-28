package bean.shareControl;

import java.io.Serializable;
import java.util.List;


/**
 * 分享页配置信息
 */
public class SharePageInfo implements Serializable {

    /**
     * 场景id
     */
    private Integer sceneCode;

    /**
     * 顶部图片链接
     */
    private String topBlockUrl;

    /**
     * 顶部跳转链接
     */
    private String topBlockLink;

    /**
     * 中间文字
     */
    private String middleChar;

    /**
     * 中间块信息
     */
    private BlockInfo middleBLock;

    /**
     * 背景图
     */
    private String bgUrl;

    /**
     * 海报文案
     */
    private String posterChar;

    /**
     * 顶部分享图片url
     */
    private String upShareViewUrl;

    /**
     * 底部分享图片url
     */
    private String downShareViewUrl;

    /**
     * 是否半透明的 1 半透明 0 否
     */
    private Integer bgcolor;

    /**
     * 分享渠道列表
     */
    private List<ShareChannelInfo> channelList;

    public Integer getSceneCode() {
        return sceneCode;
    }

    public void setSceneCode(Integer sceneCode) {
        this.sceneCode = sceneCode;
    }

    public String getTopBlockUrl() {
        return topBlockUrl;
    }

    public void setTopBlockUrl(String topBlockUrl) {
        this.topBlockUrl = topBlockUrl;
    }

    public String getTopBlockLink() {
        return topBlockLink;
    }

    public void setTopBlockLink(String topBlockLink) {
        this.topBlockLink = topBlockLink;
    }

    public String getMiddleChar() {
        return middleChar;
    }

    public void setMiddleChar(String middleChar) {
        this.middleChar = middleChar;
    }

    public BlockInfo getMiddleBLock() {
        return middleBLock;
    }

    public void setMiddleBLock(BlockInfo middleBLock) {
        this.middleBLock = middleBLock;
    }

    public String getBgUrl() {
        return bgUrl;
    }

    public void setBgUrl(String bgUrl) {
        this.bgUrl = bgUrl;
    }

    public String getPosterChar() {
        return posterChar;
    }

    public void setPosterChar(String posterChar) {
        this.posterChar = posterChar;
    }

    public String getUpShareViewUrl() {
        return upShareViewUrl;
    }

    public void setUpShareViewUrl(String upShareViewUrl) {
        this.upShareViewUrl = upShareViewUrl;
    }

    public String getDownShareViewUrl() {
        return downShareViewUrl;
    }

    public void setDownShareViewUrl(String downShareViewUrl) {
        this.downShareViewUrl = downShareViewUrl;
    }

    public Integer getBgcolor() {
        return bgcolor;
    }

    public void setBgcolor(Integer bgcolor) {
        this.bgcolor = bgcolor;
    }

    public List<ShareChannelInfo> getChannelList() {
        return channelList;
    }

    public void setChannelList(List<ShareChannelInfo> channelList) {
        this.channelList = channelList;
    }
}
