package bean.ans;

import java.io.Serializable;
import java.util.List;

/**
 * @ClassName AnsFather
 * @Description TODO
 * @Author lenovo
 * @Date 2019/7/19 15:43
 **/
public class AnsFather implements Serializable {
    private List<AnsItem> vAnsItem;

    public List<AnsItem> getvAnsItem() {
        return vAnsItem;
    }

    public void setvAnsItem(List<AnsItem> vAnsItem) {
        this.vAnsItem = vAnsItem;
    }
}
