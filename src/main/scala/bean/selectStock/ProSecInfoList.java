//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package bean.selectStock;

import com.niufu.tar.bec.GroupInfo;
import com.niufu.tar.bec.SubjectInfo;
import com.qq.tars.protocol.tars.BaseDecodeStream;
import com.qq.tars.protocol.tars.BaseEncodeStream;
import com.qq.tars.protocol.tars.Message;
import com.qq.tars.protocol.tars.annotation.TarsStruct;
import com.qq.tars.protocol.tars.annotation.TarsStructProperty;
import com.qq.tars.protocol.util.TarsUtil;
import java.util.ArrayList;
import java.util.List;

@TarsStruct
public class ProSecInfoList extends Message {
    @TarsStructProperty(
        order = 0,
        isRequire = false
    )
    public List<ProSecInfo> vProSecInfo = null;
    @TarsStructProperty(
        order = 1,
        isRequire = false
    )
    public List<SubjectInfo> vSubjectInfo = null;
    @TarsStructProperty(
        order = 2,
        isRequire = false
    )
    public int iVersion = 0;
    @TarsStructProperty(
        order = 3,
        isRequire = false
    )
    public List<GroupInfo> vGroupInfo = null;
    static List<ProSecInfo> cache_vProSecInfo = new ArrayList();
    static List<SubjectInfo> cache_vSubjectInfo;
    static List<GroupInfo> cache_vGroupInfo;

    public List<ProSecInfo> getVProSecInfo() {
        return this.vProSecInfo;
    }

    public void setVProSecInfo(List<ProSecInfo> vProSecInfo) {
        this.vProSecInfo = vProSecInfo;
    }

    public List<SubjectInfo> getVSubjectInfo() {
        return this.vSubjectInfo;
    }

    public void setVSubjectInfo(List<SubjectInfo> vSubjectInfo) {
        this.vSubjectInfo = vSubjectInfo;
    }

    public int getIVersion() {
        return this.iVersion;
    }

    public void setIVersion(int iVersion) {
        this.iVersion = iVersion;
    }

    public List<GroupInfo> getVGroupInfo() {
        return this.vGroupInfo;
    }

    public void setVGroupInfo(List<GroupInfo> vGroupInfo) {
        this.vGroupInfo = vGroupInfo;
    }

    public ProSecInfoList() {
    }

    public ProSecInfoList(List<ProSecInfo> vProSecInfo, List<SubjectInfo> vSubjectInfo, int iVersion, List<GroupInfo> vGroupInfo) {
        this.vProSecInfo = vProSecInfo;
        this.vSubjectInfo = vSubjectInfo;
        this.iVersion = iVersion;
        this.vGroupInfo = vGroupInfo;
    }


    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (!(obj instanceof ProSecInfoList)) {
            return false;
        } else {
            ProSecInfoList other = (ProSecInfoList)obj;
            return TarsUtil.equals(this.vProSecInfo, other.vProSecInfo) && TarsUtil.equals(this.vSubjectInfo, other.vSubjectInfo) && TarsUtil.equals(this.iVersion, other.iVersion) && TarsUtil.equals(this.vGroupInfo, other.vGroupInfo);
        }
    }

    public void writeTo(BaseEncodeStream os) {
        BaseEncodeStream _os = new BaseEncodeStream(os);
        _os.setCharset(os.getCharset());
        if (null != this.vProSecInfo) {
            _os.write(0, this.vProSecInfo);
        }

        if (null != this.vSubjectInfo) {
            _os.write(1, this.vSubjectInfo);
        }

        _os.write(2, this.iVersion);
        if (null != this.vGroupInfo) {
            _os.write(3, this.vGroupInfo);
        }

    }

    public void readFrom(BaseDecodeStream is) {
        BaseDecodeStream _is = new BaseDecodeStream(is);
        _is.setCharset(is.getCharset());
        this.vProSecInfo = (List)_is.read(0, false, cache_vProSecInfo);
        this.vSubjectInfo = (List)_is.read(1, false, cache_vSubjectInfo);
        this.iVersion = _is.read(2, false, this.iVersion);
        this.vGroupInfo = (List)_is.read(3, false, cache_vGroupInfo);
    }

    static {
        ProSecInfo var_422 = new ProSecInfo();
        cache_vProSecInfo.add(var_422);
        cache_vSubjectInfo = new ArrayList();
        SubjectInfo var_423 = new SubjectInfo();
        cache_vSubjectInfo.add(var_423);
        cache_vGroupInfo = new ArrayList();
        GroupInfo var_424 = new GroupInfo();
        cache_vGroupInfo.add(var_424);
    }
}
