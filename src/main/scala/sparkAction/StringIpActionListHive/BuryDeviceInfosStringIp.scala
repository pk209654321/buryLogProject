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
object BuryDeviceInfosStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionDeviceInfos")

  def cleanBuryDeviceInfosStringIp(filterData: RDD[BuryLogin], spark:SparkSession, dayFlag: Int) = {
    /**
    　　* @Description: TODO 清洗出手机客户端的数据insert到hive仓库中
    　　* @param [filterData, hc, dayFlag]
    　　* @return org.apache.spark.sql.DataFrame
    　　* @throws
    　　* @author lenovo
    　　* @date 2019/2/12 13:45
    　　*/
    val map: RDD[Row] = filterData.map(one => {
      val line = one.line
      val ipStr = one.ipStr
      val split = line.split("\\|")
      Row(line, ipStr)
    })

    val createDataFrame: DataFrame = spark.createDataFrame(map, StructUtil.structCommonStringIp)
    val reDF = createDataFrame.repartition(1)
    reDF.createOrReplaceTempView("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val hql = s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable"
    spark.sql(hql)

  }
}
