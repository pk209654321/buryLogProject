package sparkAction.StringIpActionListHive

import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin
import sparkAction.StringIpActionListHive.BuryClientWebTableStringIp.TABLE
import sparkAction.buryCleanUtil.BuryCleanCommon

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  *
  */
object BurySmallProgramActionTableStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("burySmallProgramAction")


  /**
    *
    * @param filterData filterData
    * @param spark      spark
    * @param dayFlag    dayFlag
    * @return
    */
  // TODO:  清洗小程序行为日志

  def cleanBuryStringIpDict(filterData: RDD[BuryLogin], spark: SparkSession, dayFlag: Int, dict: Array[String]) {
    val map: RDD[Row] = filterData.map(one => {
      val all: String = one.line
      val ipStr = one.ipStr
      val splits = all.split("\\|")
      val hashMap = new mutable.HashMap[String, String]()
      splits.foreach(one => {
        /*val i = one.indexOf("=") //获取第一个等于号的位置
        if (i >= 0) {
          //有等于号
          val strfirst = one.substring(0, i)
          val strSecond = one.substring(i + 1, one.length)
          val trimKey: String = strfirst.trim
          val trimVal: String = strSecond.trim
          val bool = BuryCleanCommon.selectStockField(dict, trimKey)
          if (!bool) {
            hashMap += ((trimKey, trimVal))
          }
        }*/

        val eqSplits = one.split("=", 2)
        val trimKey = eqSplits(0).trim
        val trimVal = eqSplits(1).trim
        val bool = BuryCleanCommon.selectStockField(dict, trimKey)
        if (!bool) {
          hashMap += ((trimKey, trimVal))
        }
      })
      Row(all, ipStr, hashMap)
    })
    val createDataFrame: DataFrame = spark.createDataFrame(map, StructUtil.structCommonStringIpMap)
    val value = createDataFrame.repartition(1)
    value.createOrReplaceTempView("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    spark.sql(s"insert overwrite  table $TABLE partition(hp_stat_date='$timeStr') select * from tempTable")
  }

  def main(args: Array[String]): Unit = {
    val str = "aaaa=33333=============="
    val strings = str.split("=", 2)
    println(strings.length)
  }
}
