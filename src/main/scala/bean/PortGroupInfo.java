package bean;

import java.io.Serializable;

/**
 * @ClassName PortGroupInfo
 * @Description TODO
 * @Author lenovo
 * @Date 2019/2/17 15:51
 **/
public class PortGroupInfo implements Serializable {
    private String sKey;
    private String updateTime;
    private Integer iVersion;
    private String gi_sGroupName;
    private Boolean gi_isDel;
    private Integer gi_iCreateTime;
    private Integer gi_iUpdateTime;
    private String gs_sDtSecCode;
    private Integer gs_iUpdateTime;
    private Boolean gs_isDel;

    public String getsKey() {
        return sKey;
    }

    public void setsKey(String sKey) {
        this.sKey = sKey;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public Integer getiVersion() {
        return iVersion;
    }

    public void setiVersion(Integer iVersion) {
        this.iVersion = iVersion;
    }

    public String getGi_sGroupName() {
        return gi_sGroupName;
    }

    public void setGi_sGroupName(String gi_sGroupName) {
        this.gi_sGroupName = gi_sGroupName;
    }

    public Boolean getGi_isDel() {
        return gi_isDel;
    }

    public void setGi_isDel(Boolean gi_isDel) {
        this.gi_isDel = gi_isDel;
    }

    public Integer getGi_iCreateTime() {
        return gi_iCreateTime;
    }

    public void setGi_iCreateTime(Integer gi_iCreateTime) {
        this.gi_iCreateTime = gi_iCreateTime;
    }

    public Integer getGi_iUpdateTime() {
        return gi_iUpdateTime;
    }

    public void setGi_iUpdateTime(Integer gi_iUpdateTime) {
        this.gi_iUpdateTime = gi_iUpdateTime;
    }

    public String getGs_sDtSecCode() {
        return gs_sDtSecCode;
    }

    public void setGs_sDtSecCode(String gs_sDtSecCode) {
        this.gs_sDtSecCode = gs_sDtSecCode;
    }

    public Integer getGs_iUpdateTime() {
        return gs_iUpdateTime;
    }

    public void setGs_iUpdateTime(Integer gs_iUpdateTime) {
        this.gs_iUpdateTime = gs_iUpdateTime;
    }

    public Boolean getGs_isDel() {
        return gs_isDel;
    }

    public void setGs_isDel(Boolean gs_isDel) {
        this.gs_isDel = gs_isDel;
    }
}
