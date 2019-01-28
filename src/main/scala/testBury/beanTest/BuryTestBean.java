package testBury.beanTest;

/**
 * @ClassName BuryTestBean
 * @Description TODO
 * @Author lenovo
 * @Date 2019/1/28 11:04
 **/
public class BuryTestBean {
    //case class BuryLogin(var line: String, var sendTime: String, var source: Int, var logType: Int, var ipStr: String)
    private String line;
    private String sendTime;
    private int source;
    private int logType;
    private String ipStr;

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getSendTime() {
        return sendTime;
    }

    public void setSendTime(String sendTime) {
        this.sendTime = sendTime;
    }

    public int getSource() {
        return source;
    }

    public void setSource(int source) {
        this.source = source;
    }

    public int getLogType() {
        return logType;
    }

    public void setLogType(int logType) {
        this.logType = logType;
    }

    public String getIpStr() {
        return ipStr;
    }

    public void setIpStr(String ipStr) {
        this.ipStr = ipStr;
    }
}
