package hadoopCode.hbaseFormal.ddloperation;

import hadoopCode.hbaseFormal.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import scalaUtil.DateScalaUtil;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @ClassName HbaseDML
 * @Description TODO
 * @Author lenovo
 * @Date 2019/1/23 14:14
 **/
public class HbaseDML {
    public static void main(String[] args) {
        String startTime = DateScalaUtil.getPreviousDateStr(-27, 4);
        String endTime = DateScalaUtil.getPreviousDateStr(-26, 4);
        System.out.println(startTime);
        HBaseUtil.init("");
        String partitonCode = HBaseUtil.getPartitonCode(startTime, 10);
        String start=partitonCode+"_"+startTime;
        String end=partitonCode+"_"+endTime;
        ResultScanner resultByStartEnd = HBaseUtil.getResultByStartEnd("test:bury_hbase", start, end);
        List<String> list = new ArrayList<>();
        for (Result result : resultByStartEnd) {
            byte[] row = result.getRow();
            String string = Bytes.toString(row);
            list.add(string);
        }
        System.out.println(list.size());


    }
}
