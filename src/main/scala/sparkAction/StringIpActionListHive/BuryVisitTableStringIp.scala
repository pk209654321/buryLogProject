package sparkAction.StringIpActionListHive

import java.util

import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/21.
  */
object BuryVisitTableStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableVisitAll")


  // TODO:  清洗出股掌柜访问日志insert到hive仓库中
  def cleanVisitData(filterData: RDD[BuryLogin], spark: SparkSession, dayFlag: Int): Unit = {
    val visitRow: RDD[Row] = filterData.map(one => {
      val ipStr: String = one.ipStr
      val all: String = one.line
      val sendTime = one.sendTime
      Row(all, ipStr)
    })
    val frame: DataFrame = spark.createDataFrame(visitRow, StructUtil.structCommonStringIp)
    val reDF = frame.repartition(2)
    reDF.createOrReplaceTempView("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    spark.sql(s"insert overwrite table $TABLE partition(hp_stat_date='$timeStr') select * from tempTable")
  }
}
