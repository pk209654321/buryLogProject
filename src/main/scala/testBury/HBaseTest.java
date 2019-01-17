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
        Put put = new Put(Bytes.toBytes("row001"));
        ArrayList<Put> puts = new ArrayList<>();
        put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("content"), Bytes.toBytes("wwwwwwwwwwwwwwwwwww"));
        puts.add(put);
        HBaseUtil.putByHTable("hbase_bury_test", puts);
    }
}
