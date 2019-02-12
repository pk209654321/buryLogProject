package sparkAction.portfolioHive

import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.PortfolioStr

/**
  * ClassName PortfolioHiveInsertObject
  * Description TODO
  * Author lenovo
  * Date 2019/2/12 10:04
  **/
object PortfolioHiveInsertObject {
  private val TABLE: String = ConfigurationManager.getProperty("portfolioTableTest")
  def insertPortfolioToHive(portData:RDD[PortfolioStr],hc: HiveContext,dayFlag:Int): Unit ={
    val portRow = portData.map(line => {
      Row(line.sKey, line.sValue, line.updatetime)
    })
    portRow
    val createDataFrame = hc.createDataFrame(portRow,StructUtil.structPortfolio)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    hc.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }
}
