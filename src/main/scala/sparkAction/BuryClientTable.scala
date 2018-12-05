package sparkAction

import java.util.Date

import bean.StockShopClient
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scalaUtil.{DateScalaUtil, StructUtil}

/**
  * Created by lenovo on 2018/11/16.
  *
  */
object BuryClientTable {
   private val TABLE: String = ConfigurationManager.getProperty("actionTableClient")
  def cleanClientData(filterClient: RDD[BuryLogin],hc: HiveContext,diffDay:Int) ={
    val map: RDD[Row] = filterClient.map(one => {
      val line = one.line
      val split = line.split("\\|")
      val client: StockShopClient = new StockShopClient
      split.foreach(l => {
        val splitEQ = l.split("=")
        if (splitEQ.length > 1) {
          //如果长度为2
          val trimKey:String = splitEQ(0).trim
          val trimVal: String = splitEQ(1).trim
          trimKey match {
            case "user_id" => if (StringUtils.isNotBlank(trimVal)) client.setUser_id(trimVal.toInt)
            case "guid" => if (StringUtils.isNotBlank(trimVal)) client.setGuid(trimVal)
            case "application" =>if (StringUtils.isNotBlank(trimVal)) client.setApplication(trimVal)
            case "version" =>if (StringUtils.isNotBlank(trimVal)) client.setVersion(trimVal)
            case "platform" =>if (StringUtils.isNotBlank(trimVal)) client.setPlatform(trimVal)
            case "object" =>if (StringUtils.isNotBlank(trimVal)) client.setObjectStr(trimVal)
            case "createtime" => if (StringUtils.isNotBlank(trimVal)) client.setCreatetime(trimVal.toLong)
            case "action_type" => if (StringUtils.isNotBlank(trimVal)) client.setAction_type(trimVal.toInt)
            case "is_fanhui" => if (StringUtils.isNotBlank(trimVal)) client.setIs_fanhui(trimVal.toInt)
            case "scode_id" =>if (StringUtils.isNotBlank(trimVal)) client.setScode_id(trimVal)
            case "market_id" =>if (StringUtils.isNotBlank(trimVal)) client.setMarket_id(trimVal)
            case "screen_direction" =>if (StringUtils.isNotBlank(trimVal)) client.setScreen_direction(trimVal)
            case "color" =>if (StringUtils.isNotBlank(trimVal)) client.setColor(trimVal)
            case "frameid" =>if (StringUtils.isNotBlank(trimVal)) client.setFrameid(trimVal)
            case "type" => if (StringUtils.isNotBlank(trimVal)) client.setTypeInt(trimVal.toInt)
            case "qs_id" =>if (StringUtils.isNotBlank(trimVal)) client.setQs_id(trimVal)
            case "from_frameid" =>if (StringUtils.isNotBlank(trimVal)) client.setFrom_frameid(trimVal)
            case "from_object" =>if (StringUtils.isNotBlank(trimVal)) client.setFrom_object(trimVal)
            case "from_resourceid" =>if (StringUtils.isNotBlank(trimVal)) client.setFrom_resourceid(trimVal)
            case "to_frameid" =>if (StringUtils.isNotBlank(trimVal)) client.setTo_frameid(trimVal)
            case "to_resourceid" =>if (StringUtils.isNotBlank(trimVal)) client.setTo_resourceid(trimVal)
            case "to_scode" =>if (StringUtils.isNotBlank(trimVal)) client.setTo_scode(trimVal)
            case "target_id" =>if (StringUtils.isNotBlank(trimVal)) client.setTarget_id(trimVal)
            case _ => println("client-trimKey------"+trimKey)
          }
        }
      })
      Row(client.getUser_id,
        client.getGuid,
        client.getApplication,
        client.getVersion,
        client.getPlatform,
        client.getObjectStr,
        client.getCreatetime,
        client.getAction_type,
        client.getIs_fanhui,
        client.getScode_id,
        client.getMarket_id,
        client.getScreen_direction,
        client.getColor,
        client.getFrameid,
        client.getTypeInt,
        client.getQs_id,
        client.getFrom_frameid,
        client.getFrom_object,
        client.getFrom_resourceid,
        client.getTo_frameid,
        client.getTo_resourceid,
        client.getTo_scode,
        client.getTarget_id)
    })

    val createDataFrame: DataFrame = hc.createDataFrame(map,StructUtil.structClient)
    createDataFrame.registerTempTable("StockShopClient")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(diffDay,1)
    val hql= s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopClient"
    hc.sql(hql)

  }
}
