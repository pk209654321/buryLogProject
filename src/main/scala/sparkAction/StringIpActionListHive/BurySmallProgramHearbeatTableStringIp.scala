package sparkAction.StringIpActionListHive

import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

/**
  * Created by lenovo on 2018/11/16.
  *
  */
object BurySmallProgramHearbeatTableStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("burySmallProgramHearbeat")


  /**
    *
    * @param filterData filterData
    * @param spark      spark
    * @param dayFlag    dayFlag
    * @return
    */
  // TODO: 清洗小程序心跳日志
  def cleanBuryStringIp(filterData: RDD[BuryLogin], spark: SparkSession, dayFlag: Int) {

    val map: RDD[Row] = filterData.map(one => {
      val line = one.line
      val ipStr = one.ipStr
      Row(line, ipStr)
    })

    val createDataFrame: DataFrame = spark.createDataFrame(map, StructUtil.structCommonStringIp)
    val reDF = createDataFrame.repartition(1)
    reDF.createOrReplaceTempView("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val hql = s"insert overwrite table $TABLE partition(hp_stat_date='$timeStr') select * from tempTable"
    spark.sql(hql)

  }
}
