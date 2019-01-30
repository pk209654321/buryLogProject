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
  * Created by lenovo on 2018/11/21.
  */
object BuryVisitTableStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableVisitAll")

  def cleanVisitData(filterData: RDD[util.List[BuryLogin]], hc: HiveContext, dayFlag: Int): Unit = {
    /**
    *　　* @Description: 清洗出股掌柜访问日志insert到hive仓库中
    *　　* @param [filterVisit, hc, diffDay]
    *　　* @return void
    *　　* @throws
    *　　* @author lenovo
    *　　* @date 2018/12/4 17:49
      * 　　*/
    val rddOne = filterData.flatMap(_.toArray())
    val visitRow: RDD[Row] = rddOne.map(one => {
      val login = one.asInstanceOf[BuryLogin]
      val ipStr: String = login.ipStr
      val all: String = login.line
      Row(all,ipStr)
    })
    val frame: DataFrame = hc.createDataFrame(visitRow,StructUtil.structCommonStringIpMap)
    frame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    hc.sql(s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }
}
