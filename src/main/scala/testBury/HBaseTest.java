package testBury;

import hadoopCode.hbaseFormal.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName HBaseTest
 * @Description TODO
 * @Author lenovo
 * @Date 2019/1/17 14:04
 **/
public class HBaseTest {
    public static void main(String[] args) throws Exception{
        HBaseUtil.init("");
        HBaseUtil.createTableSplit("hbase_test1", 1,"f1");
    }
}
