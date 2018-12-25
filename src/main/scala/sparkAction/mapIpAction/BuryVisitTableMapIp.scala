package sparkAction.mapIpAction

import bean.StockShopVisit
import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/21.
  */
object BuryVisitTableMapIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableVisit")

  def cleanVisitData(filterVisit: RDD[BuryLogin], hc: HiveContext, dayFlag: Int): Unit = {
    /**
    　　* @Description: 清洗出股掌柜访问日志insert到hive仓库中
    　　* @param [filterVisit, hc, diffDay]
    　　* @return void
    　　* @throws
    　　* @author lenovo
    　　* @date 2018/12/4 17:49
    　　*/
    //import hc.implicits._
    val visitRow: RDD[Row] = filterVisit.map(line => {
      val ipStr: String = line.ipStr
      //获取外网ip
      val all: String = line.line
      val split: Array[String] = all.split("\\|")
      val hashMap = new mutable.HashMap[String, String]()
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
    val frame: DataFrame = hc.createDataFrame(visitRow,StructUtil.structCommonMapIp)
    frame.registerTempTable("StockShopVisitMap")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    hc.sql(s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopVisitMap")
  }
}
