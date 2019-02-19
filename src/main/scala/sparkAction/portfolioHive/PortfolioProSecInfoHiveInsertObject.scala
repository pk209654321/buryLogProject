package sparkAction.portfolioHive

import java.util

import bean.{PortGroupInfo, PortfolioBean}
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
object PortfolioProSecInfoHiveInsertObject {
  private val TABLE: String = ConfigurationManager.getProperty("portfolioTableTest")
  private val TABLE_MANY: String = ConfigurationManager.getProperty("portfolioTableTestMany")
  private val TABLE_GROUP: String = ConfigurationManager.getProperty("portfolioTableTestGroup")
  def insertPortfolioToHive(portData:RDD[PortfolioStr],hc: HiveContext,dayFlag:Int): Unit ={
   val portRow= portData.map(line => {
      Row(line.sKey,line.sValue,line.updatetime)
    })
    val createDataFrame = hc.createDataFrame(portRow,StructUtil.structPortfolio)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    hc.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }

  def insertPortfolioManyToHive( many: RDD[PortfolioBean],hc: HiveContext,dayFlag:Int): Unit ={
    val portRow = many.map(line => {
      var broadCastTime = line.getvBroadcastTime()
      var strategyId = line.getvStrategyId()
      if(broadCastTime.size()==0){
        broadCastTime=null
      }
      if(strategyId.size()==0){
        strategyId=null
      }
      Row(
        line.getbRecvAnnounce(),
        line.getbRecvResearch(),
        line.getfChipHighPrice(),
        line.getfChipLowPrice(),
        line.getfDecreasesPer(),
        line.getfHighPrice(),
        line.getfIncreasePer(),
        line.getfLowPrice(),
        line.getfMainChipHighPrice(),
        line.getfMainChipLowPrice(),
        line.getiCreateTime(),
        line.getiUpdateTime(),
        line.getiVersion(),
        line.getsAiAlert(),
        line.getsDel(),
        line.getsDKAlert(),
        line.getsDtSecCode(),
        line.getsHold(),
        line.getsKey(),
        line.getsName(),
        line.getStCommentInfo_iCreateTime,
        line.getStCommentInfo_iUpdateTime,
        line.getStCommentInfo_sComment,
        broadCastTime,
        strategyId,
        line.getUpdateTime
      )
    })
    val createDataFrame = hc.createDataFrame(portRow,StructUtil.structPortfolioProSecInfo)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    hc.sql(s"insert overwrite  table ${TABLE_MANY} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }

  def insertPortfolioToHiveGroupInfo(portData:RDD[PortGroupInfo],hc: HiveContext,dayFlag:Int): Unit ={
    val portRow= portData.map(line => {
      Row(
        line.getGi_iCreateTime,
        line.getGi_isDel,
        line.getGi_iUpdateTime,
        line.getGi_sGroupName,
        line.getGs_isDel,
        line.getGs_iUpdateTime,
        line.getGs_sDtSecCode,
        line.getiVersion(),
        line.getsKey(),
        line.getUpdateTime
      )
    })
    val createDataFrame = hc.createDataFrame(portRow,StructUtil.structPortGroupInfo)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    hc.sql(s"insert overwrite  table ${TABLE_GROUP} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }


}
