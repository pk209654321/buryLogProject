package sparkRealTime.buryLogRealTimeOdsTables

import java.util

import com.typesafe.config.Config
import hadoopCode.hbaseFormal.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import sparkAction.BuryLogin
import sparkAction.buryCleanUtil.BuryCleanCommon

/**
  * ClassName RealTimeStockClient
  * Description TODO
  * Author lenovo
  * Date 2019/3/8 19:17
  **/
object RealTimeStockWeb {
  def doRealTimeStockWeb(buryLogins: Iterator[BuryLogin], load: Config): Unit = {
    val puts = new util.ArrayList[Put]()
    HBaseUtil.init("")

    buryLogins.foreach(one => {
      try {
        //获取日志详情
        val lineStr = one.line
        //获取日志ip
        val ip = one.ipStr
        //获取发送日志时间
        val sendTime = one.sendTime
        val timeStr = BuryCleanCommon.getDayTimeByTime(one)
        val rowKey = HBaseUtil.getVisitRowKey(2, timeStr, 10)
        val put = new Put(Bytes.toBytes(rowKey))
        val strings = lineStr.split("\\|", -1)
        val i = strings(0).indexOf("=")
        if (i >= 0) {
          //旧版日志
          strings.foreach(kv => {
            val _kv = kv.split("=")
            var key = ""
            var value = ""
            if (_kv.length == 2) {
              key = _kv(0).trim
              value = _kv(1).trim
            }
            if(StringUtils.isNotBlank(key)&&StringUtils.isNotBlank(value)){
              put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes(key), Bytes.toBytes(value))
            }
          })
          puts.add(put)
        } else {
          //客户端内嵌网页端没有去key日志
        }
      } catch {
        case e: Throwable => println("error burylog is ---------------" + one.line); e.printStackTrace()
      }
    })
    HBaseUtil.put(load.getString("hbase.table.stock.client"), puts)
  }
}
