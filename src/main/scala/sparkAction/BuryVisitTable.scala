package sparkAction

import java.util.Date

import bean.{StockShopVisit, StockShopWeb}
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

import scalaUtil.{DateScalaUtil, StructUtil}

/**
  * Created by lenovo on 2018/11/21.
  */
object BuryVisitTable {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableVisit")
  def cleanVisitData(filterVisit: RDD[BuryLogin], hc: HiveContext, diffDay:Int): Unit = {
    val map: RDD[Row] = filterVisit.map(line => {
      val ipStr: String = line.ipStr//获取外网ip
      val all: String = line.line
      val split: Array[String] = all.split("\\|")
      val visit: StockShopVisit = new StockShopVisit()
      for (i <- split) {
        val strings: Array[String] = i.split("=")
        if (strings.length > 1) {
          val trimKey: String = strings(0).trim
          val trimVal: String = strings(1).trim
          trimKey match {
            case "user_id"=> if(StringUtils.isNotBlank(trimVal)) visit.setUser_id(trimVal.toInt)
            case "guid" => if(StringUtils.isNotBlank(trimVal)) visit.setGuid(trimVal)
            case "access_time" => if(StringUtils.isNotBlank(trimVal)) visit.setAccess_time(trimVal.toLong)
            case "offline_time" => if(StringUtils.isNotBlank(trimVal)) visit.setOffline_time(trimVal.toLong)
            case "download_channel" => if(StringUtils.isNotBlank(trimVal)) visit.setDownload_channel(trimVal)
            case "client_version" => if(StringUtils.isNotBlank(trimVal)) visit.setClient_version(trimVal)
            case "phone_model" => if(StringUtils.isNotBlank(trimVal)) visit.setPhone_model(trimVal)
            case "phone_system" => if(StringUtils.isNotBlank(trimVal)) visit.setPhone_system(trimVal)
            case "system_version" => if(StringUtils.isNotBlank(trimVal)) visit.setSystem_version(trimVal)
            case "operator" => if(StringUtils.isNotBlank(trimVal)) visit.setOperator(trimVal)
            case "network" => if(StringUtils.isNotBlank(trimVal)) visit.setNetwork(trimVal)
            case "resolution" => if(StringUtils.isNotBlank(trimVal)) visit.setResolution(trimVal)
            case "screen_height" => if(StringUtils.isNotBlank(trimVal)) visit.setScreen_height(trimVal)
            case "screen_width" => if(StringUtils.isNotBlank(trimVal)) visit.setScreen_width(trimVal)
            case "mac" => if(StringUtils.isNotBlank(trimVal)) visit.setMac(trimVal)
            case "ip" => if(StringUtils.isNotBlank(trimVal)) visit.setIp(ipStr)
            case "imei" => if(StringUtils.isNotBlank(trimVal)) visit.setImei(trimVal)
            case "iccid" => if(StringUtils.isNotBlank(trimVal)) visit.setIccid(trimVal)
            case "meid" => if(StringUtils.isNotBlank(trimVal)) visit.setMeid(trimVal)
            case "idfa" => if(StringUtils.isNotBlank(trimVal)) visit.setIdfa(trimVal)
            case _ =>println("visit-trimKey------"+trimKey)
          }
        }
      }
      Row(visit.getUser_id,
        visit.getGuid,
        visit.getAccess_time,
        visit.getOffline_time,
        visit.getDownload_channel,
        visit.getClient_version,
        visit.getPhone_model,
        visit.getPhone_system,
        visit.getSystem_version,
        visit.getOperator,
        visit.getNetwork,
        visit.getResolution,
        visit.getScreen_height,
        visit.getScreen_width,
        visit.getMac,
        visit.getIp,
        visit.getImei,
        visit.getIccid,
        visit.getMeid,
        visit.getIdfa)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(map, StructUtil.structVisit)
    createDataFrame.registerTempTable("StockShopVisit")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(diffDay,1)
    hc.sql(s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopVisit")
  }
}
