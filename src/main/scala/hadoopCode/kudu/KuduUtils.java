package hadoopCode.kudu;

import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class KuduUtils {
    private static final ThreadLocal<KuduSession> threadLocal = new ThreadLocal<>();
    private static Logger logger = LoggerFactory.getLogger(hadoopCode.kudu.KuduUtils.class);
    private static Map<String,Object> map=new ConcurrentHashMap<>();

    public static KuduTable table(String name) throws KuduException {
        return Kudu.INSTANCE.table(name);
    }

    public static Insert emptyInsert(String table) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        return ktable.newInsert();
    }

    public static Update emptyUpdate(String table) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        return ktable.newUpdate();
    }

    public static Upsert emptyUpsert(String table) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        return ktable.newUpsert();
    }

    /**
     * Only columns which are part of the key can be set
     *
     * @param table
     * @return
     */
    public static Delete emptyDelete(String table) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        return ktable.newDelete();
    }


    public static Delete createDeleteNew(String table, JSONObject data) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        Delete delete = ktable.newDelete();
        PartialRow row = delete.getRow();
        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            try {
                ColumnSchema colSchema = schema.getColumn(colName);
                if (colSchema.isKey()) {
                    fillRow(row, colSchema, data);
                }
            } catch (Exception e) {
                logger.info("=====字段:" + colName + "不存在于kudu表:" + table);
            }
        }
        return delete;
    }

    public static Delete createDelete(String table, JSONObject data, String pk) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        Delete delete = ktable.newDelete();
        PartialRow row = delete.getRow();
        Schema schema = ktable.getSchema();
        String[] splits = pk.split(",", -1);
        for (String colName : data.keySet()) {
            for (String split : splits) {
                if (colName.equals(split.trim())) {
                    ColumnSchema colSchema = schema.getColumn(colName);
                    fillRow(row, colSchema, data);
                }
            }
        }
        return delete;
    }


    public static Upsert createUpsert(String table, JSONObject data) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        Upsert upsert = ktable.newUpsert();
        PartialRow row = upsert.getRow();
        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            try {//todo  try目的避免缺字段导致的报错
                ColumnSchema colSchema = schema.getColumn(colName);
                fillRow(row, colSchema, data);
            } catch (Exception e) {
                logger.info("=====字段:" + colName + "不存在于kudu表:" + table);
            }
        }
        return upsert;
    }

    public static Insert createInsert(String table, JSONObject data) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        Insert insert = ktable.newInsert();
        PartialRow row = insert.getRow();
        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            ColumnSchema colSchema = schema.getColumn(colName);
            fillRow(row, colSchema, data);
        }
        return insert;
    }

    public static void upsert(String table, JSONObject data) throws KuduException {
        Upsert upsert = createUpsert(table, data);
        KuduSession session = getSession();
        session.apply(upsert);
        session.flush();
        closeSession();
    }

    public static void insert(String table, JSONObject data) throws KuduException {
        Insert insert = createInsert(table, data);
        KuduSession session = getSession();
        session.apply(insert);
        session.flush();
        closeSession();
    }


    //TODO: 2019/9/24 修改表结构
    public static void dropExampleTable(KuduClient client, String tableName) throws KuduException {
        client.deleteTable(tableName);
    }

    public static void alterTableAddColumn(String tableName, String column, Type type) throws KuduException {
        KuduClient client = Kudu.INSTANCE.client();
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        alterTableOptions.addColumn(new ColumnSchema.ColumnSchemaBuilder(column, type).nullable(true).build());
        client.alterTable(tableName, alterTableOptions);
    }

    public static void alterTableDeleteColumn(String tableName, String column) throws KuduException {
        KuduClient client = Kudu.INSTANCE.client();
        AlterTableOptions alterTableOptions = new AlterTableOptions().dropColumn(column);
        client.alterTable(tableName, alterTableOptions);
    }

    public static void alterTableChangeColumn(String tableName, String oldName, String newName) throws KuduException {
        KuduClient client = Kudu.INSTANCE.client();
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        alterTableOptions.renameColumn(oldName, newName);
        client.alterTable(tableName, alterTableOptions);
    }


    private static void fillRow(PartialRow row, ColumnSchema colSchema, JSONObject data) {
        String name = colSchema.getName();
        if (data.get(name) == null) {
            return;
        }
        Type type = colSchema.getType();
        switch (type) {
            case STRING:
                row.addString(name, data.getString(name));
                break;
            case INT64:
            case UNIXTIME_MICROS:
                row.addLong(name, data.getLongValue(name));
                break;
            case DOUBLE:
                row.addDouble(name, data.getDoubleValue(name));
                break;
            case INT32:
                row.addInt(name, data.getIntValue(name));
                break;
            case INT16:
                row.addShort(name, data.getShortValue(name));
                break;
            case INT8:
                row.addByte(name, data.getByteValue(name));
                break;
            case BOOL:
                row.addBoolean(name, data.getBooleanValue(name));
                break;
            case BINARY:
                row.addBinary(name, data.getBytes(name));
                break;
            case FLOAT:
                row.addFloat(name, data.getFloatValue(name));
                break;
            default:
                break;
        }
    }

    public static KuduSession getSession() throws KuduException {
        KuduSession session = threadLocal.get();
        if (session == null) {
            session = Kudu.INSTANCE.newSession();
            threadLocal.set(session);
        }
        return session;
    }

    public static KuduSession getManualSession() throws KuduException {
        KuduSession session = threadLocal.get();
        if (session == null) {
            session = Kudu.INSTANCE.newSessionManUal();
            threadLocal.set(session);
        }
        return session;
    }

    public static KuduSession getAsyncSession() throws KuduException {
        KuduSession session = threadLocal.get();
        if (session == null) {
            session = Kudu.INSTANCE.newAsyncSession();
            threadLocal.set(session);
        }
        return session;
    }

    public static void renameImpalaKuduField(HashMap<String, String> map, String tableName) {
        /**
         　　* @Description: TODO 批量修改kudu字段名称
         　　* @param [map, tableName]
         　　* @return void
         　　* @throws
         　　* @author lenovo
         　　* @date 2019/9/26 15:07
         　　*/
        map.forEach((k, v) -> {
            try {
                alterTableChangeColumn(tableName, k, v);
            } catch (KuduException e) {
                e.printStackTrace();
            }
        });
    }

    public static void closeSession() {
        KuduSession session = threadLocal.get();
        threadLocal.set(null);
        Kudu.INSTANCE.closeSession(session);
    }

    public static void main(String[] args) throws KuduException {
        /*alterTableDeleteColumn("impala::kudu_real.t_user_pay_record", "clientorderuuid");
        Type string = Type.STRING;
        alterTableAddColumn("impala::kudu_real.t_user_pay_record", "clientOrderUUID", string);*/
        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put("data_id", "DATA_ID");
        stringStringHashMap.put("data_key", "DATA_KEY");
        stringStringHashMap.put("user_type", "USER_TYPE");
        stringStringHashMap.put("user_id", "USER_ID");
        stringStringHashMap.put("circle_id", "CIRCLE_ID");
        stringStringHashMap.put("circle_tab_id", "CIRCLE_TAB_ID");
        stringStringHashMap.put("from_id", "FROM_ID");
        stringStringHashMap.put("from_guid", "FROM_GUID");
        stringStringHashMap.put("from_sign_id", "FROM_SIGN_ID");
        stringStringHashMap.put("data_comment", "DATA_COMMENT");
        stringStringHashMap.put("update_time", "UPDATE_TIME");

        renameImpalaKuduField(stringStringHashMap, "impala::kudu_real.nf_ac_customer_advisor_qrcode_info");
    }
}
