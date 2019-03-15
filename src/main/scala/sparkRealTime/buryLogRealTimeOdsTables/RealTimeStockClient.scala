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
object RealTimeStockClient {
  def doRealTimeStockClient(buryLogins: Iterator[BuryLogin], load: Config): Unit = {
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
            key match {
              case "user_id" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("user_id"), Bytes.toBytes(value))
              case "guid" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("guid"), Bytes.toBytes(value))
              case "application" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("application"), Bytes.toBytes(value))
              case "version" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("version"), Bytes.toBytes(value))
              case "platform" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("platform"), Bytes.toBytes(value))
              case "object" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("object"), Bytes.toBytes(value))
              case "createtime" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("createtime"), Bytes.toBytes(value))
              case "action_type" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("action_type"), Bytes.toBytes(value))
              case "is_fanhui" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("is_fanhui"), Bytes.toBytes(value))
              case "scode_id" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("scode_id"), Bytes.toBytes(value))
              case "market_id" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("market_id"), Bytes.toBytes(value))
              case "screen_direction" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("screen_direction"), Bytes.toBytes(value))
              case "color" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("color"), Bytes.toBytes(value))
              case "frameid" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("frameid"), Bytes.toBytes(value))
              case "type" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("type"), Bytes.toBytes(value))
              case "qs_id" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("qs_id"), Bytes.toBytes(value))
              case "from_frameid" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("from_frameid"), Bytes.toBytes(value))
              case "from_object" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("from_object"), Bytes.toBytes(value))
              case "from_resourceid" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("from_resourceid"), Bytes.toBytes(value))
              case "to_frameid" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("to_frameid"), Bytes.toBytes(value))
              case "to_resourceid" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("to_resourceid"), Bytes.toBytes(value))
              case "to_scode" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("to_scode"), Bytes.toBytes(value))
              case "target_id" => if (StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("target_id"), Bytes.toBytes(value))
              case _ => if (StringUtils.isNotBlank(key)) println("其他key-----" + key)
            }
          })
          puts.add(put)
        } else {
          //新版日志
          if (StringUtils.isNotBlank(strings(0))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("user_id"), Bytes.toBytes(strings(0)))
          if (StringUtils.isNotBlank(strings(1))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("guid"), Bytes.toBytes(strings(1)))
          if (StringUtils.isNotBlank(strings(2))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("application"), Bytes.toBytes(strings(2)))
          if (StringUtils.isNotBlank(strings(3))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("version"), Bytes.toBytes(strings(3)))
          if (StringUtils.isNotBlank(strings(4))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("platform"), Bytes.toBytes(strings(4)))
          if (StringUtils.isNotBlank(strings(5))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("object"), Bytes.toBytes(strings(5)))
          if (StringUtils.isNotBlank(strings(6))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("createtime"), Bytes.toBytes(strings(6)))
          if (StringUtils.isNotBlank(strings(7))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("action_type"), Bytes.toBytes(strings(7)))
          if (StringUtils.isNotBlank(strings(8))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("is_fanhui"), Bytes.toBytes(strings(8)))
          if (StringUtils.isNotBlank(strings(9))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("scode_id"), Bytes.toBytes(strings(9)))
          if (StringUtils.isNotBlank(strings(10))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("market_id"), Bytes.toBytes(strings(10)))
          if (StringUtils.isNotBlank(strings(11))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("screen_direction"), Bytes.toBytes(strings(11)))
          if (StringUtils.isNotBlank(strings(12))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("color"), Bytes.toBytes(strings(12)))
          if (StringUtils.isNotBlank(strings(13))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("frameid"), Bytes.toBytes(strings(13)))
          if (StringUtils.isNotBlank(strings(14))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("type"), Bytes.toBytes(strings(14)))
          if (StringUtils.isNotBlank(strings(15))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("qs_id"), Bytes.toBytes(strings(15)))
          if (StringUtils.isNotBlank(strings(16))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("from_frameid"), Bytes.toBytes(strings(16)))
          if (StringUtils.isNotBlank(strings(17))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("from_object"), Bytes.toBytes(strings(17)))
          if (StringUtils.isNotBlank(strings(18))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("from_resourceid"), Bytes.toBytes(strings(18)))
          if (StringUtils.isNotBlank(strings(19))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("to_frameid"), Bytes.toBytes(strings(19)))
          if (StringUtils.isNotBlank(strings(20))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("to_resourceid"), Bytes.toBytes(strings(20)))
          if (StringUtils.isNotBlank(strings(21))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("to_scode"), Bytes.toBytes(strings(21)))
          if (StringUtils.isNotBlank(strings(22))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("target_id"), Bytes.toBytes(strings(22)))
          puts.add(put)
        }
      } catch {
        case e: Throwable => println("error burylog is ---------------" + one.line); e.printStackTrace()
      }
    })
    HBaseUtil.put(load.getString("hbase.table.stock.client"), puts)
  }
}
