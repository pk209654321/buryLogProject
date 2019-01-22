package hadoopCode.hbaseFormal.ddloperation;

import hadoopCode.hbaseFormal.util.HBaseUtil;

/**
 * @ClassName HbaseDDL
 * @Description TODO
 * @Author lenovo
 * @Date 2019/1/18 13:40
 **/
public class HbaseDDL {

    public static void main(String[] args){
        try {
            HBaseUtil.init("");
            HBaseUtil.createTableSplit("test:bury_hbase", 10,"f1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
