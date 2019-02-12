package sparkAction.StringIpActionListHive

import java.util

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
object BuryClientWebTableStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableClientWebAll")

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
      Row(all, ipStr,null)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(map, StructUtil.structCommonStringIpMap)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    hc.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }
}
