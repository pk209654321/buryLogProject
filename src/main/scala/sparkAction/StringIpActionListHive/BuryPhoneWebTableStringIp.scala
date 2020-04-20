package sparkAction.StringIpActionListHive

import java.util

import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  *
  */
object BuryPhoneWebTableStringIp {
   private val TABLE: String = ConfigurationManager.getProperty("actionTablePhoneWebAll")

  // TODO:  清洗出手机浏览器端的日志
  def cleanPhoneWebData(filterData: RDD[BuryLogin], hc: HiveContext, dayFlag: Int):sql.DataFrame ={
    val map: RDD[Row] = filterData.map(one => {
      val line = one.line
      val ipStr = one.ipStr
      Row(line,ipStr)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(map,StructUtil.structCommonStringIpMap)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    val hql= s"insert overwrite table $TABLE partition(hp_stat_date='$timeStr') select * from tempTable"
    hc.sql(hql)
  }
}
