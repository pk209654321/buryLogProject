package sparkAction.userLoginReport

import bean.StockShopWeb
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  */
object BuryWebTableMap {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableWeb")
  def cleanWebData(filterWeb: RDD[BuryLogin], hc: HiveContext,diffDay:Int): Unit = {
    /**
    　　* @Description: 清洗出WEB端的数据
    　　* @param [filterWeb, hc, diffDay]
    　　* @return void
    　　* @throws
    　　* @author lenovo
    　　* @date 2018/12/4 17:45
    　　*/
    val map: RDD[Row] = filterWeb.map(line => {
      val all: String = line.line
      val split: Array[String] = all.split("\\|")
      val web: StockShopWeb = new StockShopWeb
      val hashMap = new mutable.HashMap[String,String]()
      for (i <- split) {
        val strings: Array[String] = i.split("=")
        if (strings.length > 1) {
          val trimKey: String = strings(0).trim
          val trimVal: String = strings(1).trim
          hashMap.+=((trimKey,trimVal))
        }
      }
      Row(hashMap)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(map, StructUtil.structWebMap)
    createDataFrame.registerTempTable("StockShopWebMap")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(diffDay,1)
    hc.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopWebMap")
  }
}
