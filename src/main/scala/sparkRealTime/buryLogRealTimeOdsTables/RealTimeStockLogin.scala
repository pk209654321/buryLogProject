package sparkRealTime.buryLogRealTimeOdsTables

import java.util

import hadoopCode.hbaseFormal.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import sparkAction.BuryLogin
import sparkAction.buryCleanUtil.BuryCleanCommon
import com.typesafe.config.{Config, ConfigFactory}
/**
  * ClassName RealTimeStockLogin
  * Description TODO
  * Author lenovo
  * Date 2019/3/8 17:53
  **/
object RealTimeStockLogin {
  def doRealTimeStockLogin( buryLogins: Iterator[BuryLogin], load: Config): Unit ={
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
        val strings = lineStr.split("\\|",-1)
        val i = strings(0).indexOf("=")
        if (i >= 0) { //旧版日志
          strings.foreach(kv => {
            val _kv = kv.split("=")
            var key=""
            var value=""
            if(_kv.length==2){
              key = _kv(0).trim
              value = _kv(1).trim
            }
            key match {
              case "user_id" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("user_id"), Bytes.toBytes(value))
              case "guid" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("guid"), Bytes.toBytes(value))
              case "access_time" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("access_time"), Bytes.toBytes(value))
              case "offline_time" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("offline_time"), Bytes.toBytes(value))
              case "download_channel" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("download_channel"), Bytes.toBytes(value))
              case "client_version" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("client_version"), Bytes.toBytes(value))
              case "phone_model" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("phone_model"), Bytes.toBytes(value))
              case "phone_system" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("phone_system"), Bytes.toBytes(value))
              case "system_version" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("system_version"), Bytes.toBytes(value))
              case "operator" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("operator"), Bytes.toBytes(value))
              case "network" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("network"), Bytes.toBytes(value))
              case "resolution" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("resolution"), Bytes.toBytes(value))
              case "screen_height" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("screen_height"), Bytes.toBytes(value))
              case "screen_width" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("screen_width"), Bytes.toBytes(value))
              case "mac" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mac"), Bytes.toBytes(value))
              case "imei" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("imei"), Bytes.toBytes(value))
              case "iccid" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("iccid"), Bytes.toBytes(value))
              case "meid" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("meid"), Bytes.toBytes(value))
              case "idfa" =>if(StringUtils.isNotBlank(value)) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("idfa"), Bytes.toBytes(value))
              case _ => if(StringUtils.isNotBlank(key)) println("其他key-----"+key)
            }
          })
          puts.add(put)
        } else { //新版日志
          if(StringUtils.isNotBlank(strings(0))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("user_id"), Bytes.toBytes(strings(0)))
          if(StringUtils.isNotBlank(strings(1))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("guid"), Bytes.toBytes(strings(1)))
          if(StringUtils.isNotBlank(strings(2))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("access_time"), Bytes.toBytes(strings(2)))
          if(StringUtils.isNotBlank(strings(3))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("offline_time"), Bytes.toBytes(strings(3)))
          if(StringUtils.isNotBlank(strings(4))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("download_channel"), Bytes.toBytes(strings(4)))
          if(StringUtils.isNotBlank(strings(5))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("client_version"), Bytes.toBytes(strings(5)))
          if(StringUtils.isNotBlank(strings(6))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("phone_model"), Bytes.toBytes(strings(6)))
          if(StringUtils.isNotBlank(strings(7))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("phone_system"), Bytes.toBytes(strings(7)))
          if(StringUtils.isNotBlank(strings(8))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("system_version"), Bytes.toBytes(strings(8)))
          if(StringUtils.isNotBlank(strings(9))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("operator"), Bytes.toBytes(strings(9)))
          if(StringUtils.isNotBlank(strings(10))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("network"), Bytes.toBytes(strings(10)))
          if(StringUtils.isNotBlank(strings(11))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("resolution"), Bytes.toBytes(strings(11)))
          if(StringUtils.isNotBlank(strings(12))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("screen_height"), Bytes.toBytes(strings(12)))
          if(StringUtils.isNotBlank(strings(13))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("screen_width"), Bytes.toBytes(strings(13)))
          if(StringUtils.isNotBlank(strings(14))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mac"), Bytes.toBytes(strings(14)))
          if(StringUtils.isNotBlank(strings(15))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("imei"), Bytes.toBytes(strings(15)))
          if(StringUtils.isNotBlank(strings(16))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("iccid"), Bytes.toBytes(strings(16)))
          if(StringUtils.isNotBlank(strings(17))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("meid"), Bytes.toBytes(strings(17)))
          if(StringUtils.isNotBlank(strings(18))) put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("idfa"), Bytes.toBytes(strings(18)))
          puts.add(put)
        }
      } catch {
        case e:Throwable => println("error burylog is ---------------"+one.line);e.printStackTrace()
      }
    })
    HBaseUtil.put(load.getString("hbase.table"),puts)
  }
}
