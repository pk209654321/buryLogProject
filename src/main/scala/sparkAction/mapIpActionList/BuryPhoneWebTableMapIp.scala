package sparkAction.mapIpActionList

import java.util

import bean.StockShopClient
import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  *
  */
object BuryPhoneWebTableMapIp {
   private val TABLE: String = ConfigurationManager.getProperty("actionTablePhoneWeb")
  def cleanPhoneWebData(filterData: RDD[BuryLogin], hc: HiveContext, dayFlag: Int) ={
    /**
    *　　* @Description: TODO 清洗出手机浏览器端的日志
    *　　* @param [filterClient, hc, dayFlag]
    *　　* @return org.apache.spark.sql.DataFrame
    *　　* @throws
    *　　* @author lenovo
    *　　* @date 2018/12/20 13:42
      * 　　*/
    val map: RDD[Row] = filterData.map(one => {
      val line = one.line
      val ipStr = one.ipStr
      val split = line.split("\\|")
      val hashMap: mutable.Map[String, String] = new mutable.HashMap[String,String]()
      split.foreach(l => {
        val i = l.indexOf("=")
        if (i > 0) {
          //如果长度为2
          val strfirst = l.substring(0,i)
          val strSecond = l.substring(i+1,l.length)
          val trimKey: String = strfirst.trim
          val trimVal: String = strSecond.trim
          hashMap += ((trimKey, trimVal))
        }
      })
      Row(hashMap,ipStr)
    })

    val createDataFrame: DataFrame = hc.createDataFrame(map,StructUtil.structCommonMapIp)
    createDataFrame.registerTempTable("StockShopPhoneWebMap")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    val hql= s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopPhoneWebMap"
    hc.sql(hql)

  }
}
