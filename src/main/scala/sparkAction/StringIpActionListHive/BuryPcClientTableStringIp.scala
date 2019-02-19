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
  *
  */
object BuryPcClientTableStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTablePcClientAll")


  def cleanPcClientData(filterData: RDD[util.List[BuryLogin]], hc: HiveContext, dayFlag: Int) = {
    /**
    　　* @Description: TODO 清洗出客户端日志
    　　* @param [filterData, hc, dayFlag]
    　　* @return org.apache.spark.sql.DataFrame
    　　* @throws
    　　* @author lenovo
    　　* @date 2019/2/12 13:44
    　　*/
    val rddOne = filterData.flatMap(_.toArray())
    val map: RDD[Row] = rddOne.map(one => {
      val login = one.asInstanceOf[BuryLogin]
      val line = login.line
      val ipStr = login.ipStr
      Row(line, ipStr)
    })

    val createDataFrame: DataFrame = hc.createDataFrame(map, StructUtil.structCommonMapIp)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val hql = s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable"
    hc.sql(hql)

  }
}