package hadoopCode.hbaseCommon;/**
 * @Auther: lenovo
 * @Date: 2018/12/19 19:56
 * @Description:
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName HBaseTest
 * @Description TODO
 * @Author lenovo
 * @Date 2018/12/19 19:56
 **/
public class HBaseTest {
    private static Configuration conf = null;
    private static Connection con = null;
    private static Admin admin = null;
    private static Logger logger = LoggerFactory.getLogger(HBaseTest.class);

    static {
        //使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "leader");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public synchronized static Connection getConnection() {
        /**
         　　* @Description: TODO 获取hbase链接
         　　* @param []
         　　* @return org.apache.hadoop.hbase.client.Connection
         　　* @throws
         　　* @author lenovo
         　　* @date 2018/12/19 20:04
         　　*/
        try {
            if (null == con || con.isClosed()) {
                // 获得连接对象
                con = ConnectionFactory.createConnection(conf);
            }
        } catch (IOException e) {
            logger.error("获取hbase链接失败");
            e.printStackTrace();
        } finally {
        }
        return con;
    }

    public static void close() {
        /**
         　　* @Description: TODO 关闭hbase链接
         　　* @param []
         　　* @return void
         　　* @throws
         　　* @author lenovo
         　　* @date 2018/12/19 20:38
         　　*/
        try {
            if (admin != null) {
                admin.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (IOException e) {
            logger.error("连接关闭失败");
            e.printStackTrace();
        }
    }

    public static boolean isTableExist(String tableName) {
        /**
         　　* @Description: TODO 判断表是否存在
         　　* @param [tableName]
         　　* @return boolean
         　　* @throws
         　　* @author lenovo
         　　* @date 2018/12/19 19:59
         　　*/
        // 创建表名对象
        TableName tn = TableName.valueOf(tableName);
        // 获取会话
        boolean b = false;
        try {
            admin = getConnection().getAdmin();
            b = admin.tableExists(tn);
        } catch (IOException e) {
            logger.error("判断hbase表是否存在失败");
            e.printStackTrace();
        }
        if (b) {
            logger.debug("hbase表:" + tableName + "   存在");
            return true;
        } else {
            logger.debug("hbase表:" + tableName + "  不存在");
            return false;
        }
    }

    public static void createTable(String tableName, String[] columnFamily) {
        /**
         　　* @Description: TODO
         　　* @param [tableName, columnFamily]
         　　* @return void
         　　* @throws
         　　* @author lenovo
         　　* @date 2018/12/19 20:16
         　　*/
        if (null == tableName || tableName.length() == 0) {
            return;
        }
        if (null == columnFamily || columnFamily.length == 0) {
            return;
        }
        if (!isTableExist(tableName)) {//如果表不存在
            // 创建表名对象
            TableName tn = TableName.valueOf(tableName);
            // 获取会话
            try {
                admin = getConnection().getAdmin();
                // 创建表结构对象
                HTableDescriptor htd = new HTableDescriptor(tn);
                for (String str : columnFamily) {
                    // 创建列族结构对象
                    HColumnDescriptor hcd = new HColumnDescriptor(str);
                    htd.addFamily(hcd);
                }
                // 创建表
                admin.createTable(htd);
                logger.debug("hbase表:" + tableName + "  创建成功");
            } catch (IOException e) {
                logger.error("创建hbase表:" + tableName + "    失败");
                e.printStackTrace();
            } finally {
                close();
            }
        } else {
            return;
        }
    }

    public static void insert(String tableName, String rowKey, String family, String qualifier, String value) {
        /**
        　　* @Description: TODO 数据单条数据
        　　* @param [tableName, rowKey, family, qualifier, value]
        　　* @return void
        　　* @throws
        　　* @author lenovo
        　　* @date 2018/12/19 20:51
        　　*/
        Table t = null;
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            t.put(put);
            logger.debug("hbase表:"+tableName+"更新成功");
        } catch (IOException e) {
            logger.debug("hbase表:"+tableName+"更新失败");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void insertBatch(String tableName, List<?> list) {
        /**
        　　* @Description: TODO 批量插入 list中传入Json字符串
        　　* @param [tableName, list]
        　　* @return void
        　　* @throws
        　　* @author lenovo
        　　* @date 2018/12/19 21:26
        　　*/
        if (null == tableName ||tableName.length()==0) {
            return;
        }
        if( null == list || list.size() == 0){
            return;
        }
        Table t = null;
        Put put = null;
        JSONObject json = null;
        List<Put> puts = new ArrayList<Put>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            for (int i = 0, j = list.size(); i < j; i++) {
                json = (JSONObject) list.get(i);
                put = new Put(Bytes.toBytes(json.getString("rowKey")));
                put.addColumn(Bytes.toBytes(json.getString("family")),
                        Bytes.toBytes(json.getString("qualifier")),
                        Bytes.toBytes(json.getString("value")));
                puts.add(put);
            }
            t.put(puts);
            logger.debug("hbase表:"+tableName+"  更新成功");
        } catch (IOException e) {
            logger.debug("hbase表:"+tableName+"  更新失败");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void delete(String tableName, String rowKey, String family, String qualifier) {
        /**
        　　* @Description: TODO 根据相应的参数删除对应的表
        　　* @param [tableName, rowKey, family, qualifier]
        　　* @return void
        　　* @throws
        　　* @author lenovo
        　　* @date 2018/12/19 21:59
        　　*/
        if (null == tableName ||tableName.length()==0) {
            return;
        }
        if( null == rowKey || rowKey.length() == 0){
            return;
        }
        Table t = null;
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            // 如果列族不为空
            if (null != family && family.length() > 0) {
                // 如果列不为空
                if (null != qualifier && qualifier.length() > 0) {
                    del.addColumn(Bytes.toBytes(family),
                            Bytes.toBytes(qualifier));
                } else {
                    del.addFamily(Bytes.toBytes(family));
                }
            }
            t.delete(del);
        } catch (IOException e) {
            logger.error("删除hbase表:"+tableName+"    失败");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void select(String tableName) {
        /**
        　　* @Description: TODO 查询表中所有的数据
        　　* @param [tableName]
        　　* @return void
        　　* @throws
        　　* @author lenovo
        　　* @date 2018/12/19 22:35
        　　*/
        if(null==tableName||tableName.length()==0){
            return;
        }
        Table t = null;
        List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            // 读取操作
            Scan scan = new Scan();
            // 得到扫描的结果集
            ResultScanner rs = t.getScanner(scan);
            if (null == rs ) {
                return;
            }
            for (Result result : rs) {
                // 得到单元格集合
                List<Cell> cs = result.listCells();
                if (null == cs || cs.size() == 0) {
                    continue;
                }
                for (Cell cell : cs) {
                    Map<String,Object> map=new HashMap<String, Object>();
                    map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
                    map.put("timestamp", cell.getTimestamp());// 取到时间戳
                    map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// 取到列族
                    map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// 取到列
                    map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// 取到值
                    list.add(map);
                }
            }
            logger.debug("查询的数据:"+list);
        } catch (IOException e) {
            logger.error("查询数据失败");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void select(String tableName, String rowKey, String family, String qualifier) {
        /**
        　　* @Description: TODO 根据条件查询
        　　* @param [tableName, rowKey, family, qualifier]
        　　* @return void
        　　* @throws
        　　* @author lenovo
        　　* @date 2018/12/20 15:31
        　　*/
        Table t = null;
        List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            // 通过HBase中的 get来进行查询
            Get get = new Get(Bytes.toBytes(rowKey));
            // 如果列族不为空
            if (null != family && family.length() > 0) {
                // 如果列不为空
                if (null != qualifier && qualifier.length() > 0) {
                    get.addColumn(Bytes.toBytes(family),
                            Bytes.toBytes(qualifier));
                } else {
                    get.addFamily(Bytes.toBytes(family));
                }
            }
            Result r = t.get(get);
            List<Cell> cs = r.listCells();
            if (null == cs || cs.size() == 0) {
                return;
            }
            for (Cell cell : cs) {
                Map<String,Object> map=new HashMap<String, Object>();
                map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
                map.put("timestamp", cell.getTimestamp());// 取到时间戳
                map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// 取到列族
                map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// 取到列
                map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// 取到值
                list.add(map);
            }
            logger.debug("查询的数据:"+list);
        } catch (IOException e) {
            logger.error("查询失败!");
            e.printStackTrace();
        } finally {
            close();
        }
    }
    public static void main(String[] args) {
        //createTable("hbaseTable1",new String[]{"info"});创建表
        //insert("hbaseTable1","001","info","name","王亚东");单条插入
        List<JSONObject> list=new ArrayList<>();
       for (int i=0;i<10000;i++){
           HbaseBean hbaseBean = new HbaseBean();
           hbaseBean.setRowKey("00000"+i);
           hbaseBean.setFamily("info");
           hbaseBean.setQualifier("name");
           hbaseBean.setValue("wang"+i);
           JSONObject jsonObject = JSON.parseObject(JSON.toJSONString(hbaseBean));
           list.add(jsonObject);
       }
        insertBatch("hbaseTable1",list);
        HbaseBean hbaseBean = new HbaseBean();

    }

}
