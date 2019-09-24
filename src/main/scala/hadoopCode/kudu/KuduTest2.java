package hadoopCode.kudu;

import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.client.*;

/**
 * @ClassName KuduTest2
 * @Description TODO
 * @Author lenovo
 * @Date 2019/9/24 8:33
 **/
public class KuduTest2 {
    public static void main(String[] args) throws KuduException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", 14);
        jsonObject.put("name", "14");
        jsonObject.put("create_time", "2019-09-23 09:53:00");
        try {
            KuduUtils.insert("impala::default.user_test", jsonObject);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        long l = System.currentTimeMillis();
        Delete delete = KuduUtils.emptyDelete("impala::default.user_test");
        delete.getRow().addLong("id", 4);
        KuduSession session = KuduUtils.getSession();
        session.apply(delete);
        session.flush();
        KuduUtils.closeSession();
        long l1 = System.currentTimeMillis();
        long l2 = (l1 - l) / 1000;
        System.out.println(l2);
    }


    public static void queryData() throws KuduException {

//打开表
        KuduClient kuduClient = Kudu.INSTANCE.client();

        KuduTable kuduTable = kuduClient.openTable("impala::default.user_test");

//获取scanner扫描器

        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);

        KuduScanner scanner = scannerBuilder.build();


//遍历

        while (scanner.hasMoreRows()) {

            RowResultIterator rowResults = scanner.nextRows();

            while (rowResults.hasNext()) {

                RowResult result = rowResults.next();

                int id = result.getInt("id");

                System.out.print("id:" + id + " ");

            }
        }
    }
}