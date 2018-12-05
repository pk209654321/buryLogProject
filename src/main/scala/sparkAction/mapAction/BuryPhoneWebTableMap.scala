package sparkAction.mapAction

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
object BuryPhoneWebTableMap {
   private val TABLE: String = ConfigurationManager.getProperty("actionTableMobile")
  def cleanPhoneWebData(filterClient: RDD[BuryLogin],hc: HiveContext,diffDay:Int) ={
    val map: RDD[Row] = filterClient.map(one => {
      val line = one.line
      val split = line.split("\\|")
      val client: StockShopClient = new StockShopClient
      val hashMap: mutable.Map[String, String] = new mutable.HashMap[String,String]()
      split.foreach(l => {
        val splitEQ = l.split("=")
        if (splitEQ.length > 1) {
          //如果长度为2
          val trimKey:String = splitEQ(0).trim
          val trimVal: String = splitEQ(1).trim
          hashMap+=((trimKey,trimVal))
        }
      })
      Row(hashMap)
    })

    val createDataFrame: DataFrame = hc.createDataFrame(map,StructUtil.structPhoneWebMap)
    createDataFrame.registerTempTable("StockShopPhoneWebMap")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(diffDay,1)
    val hql= s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopPhoneWebMap"
    hc.sql(hql)

  }
}
