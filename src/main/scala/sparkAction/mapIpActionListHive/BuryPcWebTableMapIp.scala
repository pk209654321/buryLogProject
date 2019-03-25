package sparkAction.mapIpActionListHive

import java.util

import bean.StockShopClient
import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  *
  */
object BuryPcWebTableMapIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTablePcWeb")

  def cleanPcWebData(filterData: RDD[BuryLogin], spark:SparkSession, dayFlag: Int) = {
    /**
      * 　　* @Description: 清洗出 PC web端的数据insert到hive仓库中
      * 　　* @param [filterClient, hc, dayFlag]
      * 　　* @return org.apache.spark.sql.DataFrame
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2018/12/4 17:48
      * 　　*/
    val value = filterData.map(one => {
      val line = one.line
      //埋点数据
      val ipStr = one.ipStr
      //真实ip
      val split = line.split("\\|")
      val client: StockShopClient = new StockShopClient
      val hashMap: mutable.Map[String, String] = new mutable.HashMap[String, String]()
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
    val createDataFrame: DataFrame = spark.createDataFrame(value, StructUtil.structCommonMapIp)
    val reDF = createDataFrame.repartition(1).persist()
    reDF.createOrReplaceTempView("StockShopPcWebMapIp")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val hql = s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopPcWebMapIp"
    spark.sql(hql)

  }
}
