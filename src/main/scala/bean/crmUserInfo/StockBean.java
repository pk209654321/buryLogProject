package bean.crmUserInfo;

import java.util.List;

/**
 * @ClassName StockBean
 * @Description TODO
 * @Author lenovo
 * @Date 2019/3/14 20:09
 **/
public class StockBean {
    private String api="BuryLogRealTimeForOnLine";
    private int version=1;
    private Long timestamp=System.currentTimeMillis();
    private List<CustomLine> data;

    public String getApi() {
        return api;
    }

    public void setApi(String api) {
        this.api = api;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public List<CustomLine> getData() {
        return data;
    }

    public void setData(List<CustomLine> data) {
        this.data = data;
    }
}
