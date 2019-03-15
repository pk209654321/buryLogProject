package hadoopCode.hbaseFormal.ddloperation;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import hadoopCode.hbaseCommon.HBaseTest;
import hadoopCode.hbaseFormal.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private static Logger logger = LoggerFactory.getLogger(HbaseDML.class);

   /* public static void main(String[] args) throws Exception {
        Config load = ConfigFactory.load();
        String tableName = load.getString("hbase.table");
        String endTime = DateScalaUtil.getPreviousDateStr(3, 4);
        HBaseUtil.init("");
        String partitonCode = HBaseUtil.getPartitonCode(endTime, 2);
        String end = partitonCode + "_" + endTime;
        System.out.println(end);
        ResultScanner resultByStartEnd = HBaseUtil.getResultByStartEnd(tableName, "", end);
        Table table = HBaseUtil.getTable(tableName);
        List<String> list = new ArrayList<>();
        List<Delete> deletes = new ArrayList<>();
        for (Result result : resultByStartEnd) {
            byte[] row = result.getRow();
            Delete d = new Delete(row);
            deletes.add(d);
        }
        if (deletes.size() > 0) {
            logger.info("需要删除的大小:" + deletes.size());
        }
        table.delete(deletes);
        table.close();
        logger.info("-----------------------------------删除结束");
    }*/
}
