package sparkAction.mapIpActionList

import java.util

import bean.StockShopWeb
import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  */
object BuryClientWebTableMapIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableClientWeb")

  def cleanClientWebData(filterData: RDD[BuryLogin], hc: HiveContext, dayFlag: Int): Unit = {
    /**
      * 　　* @Description: 清洗出手机客户端镶嵌WEB的数据
      * 　　* @param [filterWeb, hc, diffDay]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2018/12/4 17:45
      * 　　*/
    val map: RDD[Row] = filterData.map(one => {
      val all: String = one.line
      val ipStr = one.ipStr
      val split: Array[String] = all.split("\\|")
      val hashMap = new mutable.HashMap[String, String]()
      split.foreach(l => {
        val i = l.indexOf("=")
        if (i > 0) {
          //如果长度为2
          val strfirst = l.substring(0, i)
          val strSecond = l.substring(i + 1, l.length)
          val trimKey: String = strfirst.trim
          val trimVal: String = strSecond.trim
          hashMap += ((trimKey, trimVal))
        }
      })
      Row(hashMap, ipStr)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(map, StructUtil.structCommonMapIp)
    createDataFrame.registerTempTable("StockShopClientWebMap")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    hc.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopClientWebMap")
  }
}
