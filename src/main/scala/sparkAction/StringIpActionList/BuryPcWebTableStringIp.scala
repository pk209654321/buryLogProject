package sparkAction.StringIpActionList

import java.util

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
object BuryPcWebTableStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTablePcWebAll")

  def cleanPcWebData(filterData: RDD[util.List[BuryLogin]], hc: HiveContext, dayFlag: Int) = {
    /**
      * 　　* @Description: 清洗出 PC web端的数据insert到hive仓库中
      * 　　* @param [filterClient, hc, dayFlag]
      * 　　* @return org.apache.spark.sql.DataFrame
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2018/12/4 17:48
      * 　　*/
    val rddOne: RDD[AnyRef] = filterData.flatMap(_.toArray())
    val value = rddOne.map(one => {
      val login = one.asInstanceOf[BuryLogin]
      //埋点数据
      val line = login.line
      //真实ip
      val ipStr = login.ipStr
      Row(line, ipStr)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(value, StructUtil.structCommonStringIpMap)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val hql = s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable"
    hc.sql(hql)

  }
}
