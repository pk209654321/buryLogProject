package sparkAction.StringIpActionList

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

  def cleanClientWebData(filterData: RDD[util.List[BuryLogin]], hc: HiveContext, dayFlag: Int): Unit = {
    /**
      * 　　* @Description: 清洗出手机客户端镶嵌WEB的数据
      * 　　* @param [filterWeb, hc, diffDay]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2018/12/4 17:45
      * 　　*/
    val rddOne = filterData.flatMap(_.toArray())
    val map: RDD[Row] = rddOne.map(one => {
      val login = one.asInstanceOf[BuryLogin]
      val all: String = login.line
      val ipStr = login.ipStr
      Row(all, ipStr)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(map, StructUtil.structCommonStringIpMap)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    hc.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }
}
