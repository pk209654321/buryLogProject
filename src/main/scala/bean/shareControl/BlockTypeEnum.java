package bean.shareControl;

/**
 * 块类型--用于渲染页面
 * create by cb 2018-8-30
 */
public enum BlockTypeEnum {

    FOREVER("H5", "H5页面"),
    WEEK("IMG", "图片"),
    MONTH("TEXT", "文案");

    private String type;
    private String desc;

    BlockTypeEnum(String type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public String getType() {
        return type;
    }

    public String getDesc() {
        return desc;
    }
}
