package bean.share;


import java.util.List;

/**
 * 分享渠道信息
 */
public class ShareChannelInfo {

	/**
	 * 分享渠道
	 *
	 */
	private Integer code;

	/**
	 * 渠道名如微信
	 */
	private String name;

	/**
	 * 分享出去的类型url链接0还是文件1（包括图片）
	 */
	private Integer shareType;

	/**
	 * 分享出去的标题
	 */
	private String shareTitle;

	/**
	 * 最终返回的某一条随机文案--对应randomTitleConfigs中的某一条
	 */
	private String randomTitle;

	/**
	 * 分享文案可配置，随机获取一条返回到前终端
	 */
	private List<String> randomTitleConfigs;

	/**
	 * 分享出去的描述信息
	 */
	private String desc;

	/**
	 * 分享网络图片
	 */
	private String shareImgUrl;

	/**
	 * 链接分享给他人
	 */
	private String link;

	/**
	 * 是否站外授权 0否-1是
	 */
	private Integer isOutsidePermission;

	/**
	 * 是否第一人称  0否-1是
	 */
	private Integer isFirstPerson;

	public Integer getCode() {
		return code;
	}

	public void setCode(Integer code) {
		this.code = code;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getShareType() {
		return shareType;
	}

	public void setShareType(Integer shareType) {
		this.shareType = shareType;
	}

	public String getShareTitle() {
		return shareTitle;
	}

	public void setShareTitle(String shareTitle) {
		this.shareTitle = shareTitle;
	}

	public String getRandomTitle() {
		return randomTitle;
	}

	public void setRandomTitle(String randomTitle) {
		this.randomTitle = randomTitle;
	}

	public List<String> getRandomTitleConfigs() {
		return randomTitleConfigs;
	}

	public void setRandomTitleConfigs(List<String> randomTitleConfigs) {
		this.randomTitleConfigs = randomTitleConfigs;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getShareImgUrl() {
		return shareImgUrl;
	}

	public void setShareImgUrl(String shareImgUrl) {
		this.shareImgUrl = shareImgUrl;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public Integer getIsOutsidePermission() {
		return isOutsidePermission;
	}

	public void setIsOutsidePermission(Integer isOutsidePermission) {
		this.isOutsidePermission = isOutsidePermission;
	}

	public Integer getIsFirstPerson() {
		return isFirstPerson;
	}

	public void setIsFirstPerson(Integer isFirstPerson) {
		this.isFirstPerson = isFirstPerson;
	}
}
