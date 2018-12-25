package hadoopCode.hbaseCommon;/**
 * @Auther: lenovo
 * @Date: 2018/12/19 21:15
 * @Description:
 */


import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName HbaseBean
 * @Description TODO
 * @Author lenovo
 * @Date 2018/12/19 21:15
 **/
public class HbaseBean {
    private String rowKey;
    private String family;
    private String qualifier;
    private String value;

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
