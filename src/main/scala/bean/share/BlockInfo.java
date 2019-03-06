package bean.share;


/**
 * 块信息
 */

public class BlockInfo {

	/**
	 * 顶部模块链接
	 *
	 */
	private String type;

	/**
	 * 内容
	 */
	private String content;

	/**
	 * 文案颜色    type为TEXT才生效
	 */
	private String textColor;

	/**
	 * 跳转链接    type为IMG时才生效
	 */
	private String contentLink;


	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getTextColor() {
		return textColor;
	}

	public void setTextColor(String textColor) {
		this.textColor = textColor;
	}

	public String getContentLink() {
		return contentLink;
	}

	public void setContentLink(String contentLink) {
		this.contentLink = contentLink;
	}
}
