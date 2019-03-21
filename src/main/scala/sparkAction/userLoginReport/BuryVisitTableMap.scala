package sparkAction.userLoginReport

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
object BuryVisitTableMap {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableVisit")

  def cleanVisitData(filterVisit: RDD[BuryLogin], hc: HiveContext, diffDay: Int): Unit = {
    /**
    　　* @Description: 清洗出访问日志insert到hive仓库中
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
      val visit: StockShopVisit = new StockShopVisit()
      val map = new mutable.HashMap[String, String]()
      for (i <- split) {
        val strings: Array[String] = i.split("=")
        if (strings.length > 1) {
          var trimKey: String = strings(0).trim
          var trimVal: String = strings(1).trim
          trimKey match {
            case "ip" => trimVal = ipStr //设置为外网ip
            case _ =>
          }
          map.+=((trimKey, trimVal))
        }
      }
      Row(map)
    })
    val frame: DataFrame = hc.createDataFrame(visitRow,StructUtil.structVisitMap)
    frame.registerTempTable("StockShopVisitMap")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(diffDay,1)
    hc.sql(s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopVisitMap")
  }
}
