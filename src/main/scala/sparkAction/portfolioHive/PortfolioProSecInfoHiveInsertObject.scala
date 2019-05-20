package sparkAction.portfolioHive

import java.util

import bean.earlyWarning.UserStockAlertCfgDataAll
import bean.shareControl.ShareMany
import bean.userChoiceStock.{PortGroupInfo, PortfolioBean}
import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.PortfolioStr

import scala.collection.{JavaConversions, mutable}

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
  private val TABLE_SHARE: String = ConfigurationManager.getProperty("portfolioTableTestShare")
  private val TABLE_WARN:String=ConfigurationManager.getProperty("portfolioTableTestEarlyWarn")

  def insertPortfolioToHive2(portData: RDD[(String,String,String)], spark: SparkSession, dayFlag: Int): Unit = {
    val portRow = portData.map(line => {
      Row(line._1, line._2, line._3)
    })
    val two = dayFlag - 1
    val createDataFrame = spark.createDataFrame(portRow, StructUtil.structPortfolio).repartition(1).persist()
    createDataFrame.createOrReplaceTempView("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val sqlStr =
      s"""
         |
         | select
         |   a.sKey,
         |   a.sValue,
         |   a.updatetime
         | from ${TABLE} a
         | left join  tempTable b
         | on a.sKey=b.sKey
         | where a.hp_stat_date=date_add(current_date(),${two})
         | and b.sValue is null
         |
         | UNION ALL
         |
         | select * from tempTable
         |
      """.stripMargin
    //spark.sql(sqlStr).repartition(1).createOrReplaceTempView("tempTable2")
    //spark.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable2")
    spark.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }
  def insertPortfolioToHive(portData: RDD[PortfolioStr], spark: SparkSession, dayFlag: Int): Unit = {
    val portRow = portData.map(line => {
      Row(line.sKey, line.sValue, line.updatetime)
    })
    val two = dayFlag - 1
    val createDataFrame = spark.createDataFrame(portRow, StructUtil.structPortfolio).repartition(1).persist()
    createDataFrame.createOrReplaceTempView("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val sqlStr =
      s"""
         |
         | select
         |   a.sKey,
         |   a.sValue,
         |   a.updatetime
         | from ${TABLE} a
         | left join  tempTable b
         | on a.sKey=b.sKey
         | where a.hp_stat_date=date_add(current_date(),${two})
         | and b.sValue is null
         |
         | UNION ALL
         |
         | select * from tempTable
         |
      """.stripMargin
    //spark.sql(sqlStr).repartition(1).createOrReplaceTempView("tempTable2")
    //spark.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable2")
    spark.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }

  def insertPortfolioManyToHive(many: RDD[PortfolioBean], spark: SparkSession, dayFlag: Int): Unit = {
    val portRow = many.map(line => {
      import scala.collection.JavaConverters._
      val broadCastTime = line.getvBroadcastTime() match {
        case null => null
        case _ =>
          val integers = line.getvBroadcastTime().asScala
          if (integers.isEmpty) {
            null
          } else {
            integers
          }
      }

      val strategyId = line.getvStrategyId() match {
        case null => null
        case _ =>
          val integers = line.getvStrategyId().asScala
          if(integers.isEmpty){
            null
          }else{
            integers
          }
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
    val createDataFrame = spark.createDataFrame(portRow, StructUtil.structPortfolioProSecInfo).repartition(1).persist()
    createDataFrame.createOrReplaceTempView("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val two = dayFlag - 1
    val sqlStr =
      s"""
         | select
         |   a.bRecvAnnounce,
         |   a.bRecvResearch,
         |   a.fChipHighPrice,
         |   a.fChipLowPrice,
         |   a.fDecreasesPer,
         |   a.fHighPrice,
         |   a.fIncreasePer,
         |   a.fLowPrice,
         |   a.fMainChipHighPrice,
         |   a.fMainChipLowPrice,
         |   a.iCreateTime,
         |   a.iUpdateTime,
         |   a.iVersion,
         |   a.isAiAlert,
         |   a.isDel,
         |   a.isDKAlert,
         |   a.sDtSecCode,
         |   a.isHold,
         |   a.sKey,
         |   a.sName,
         |   a.stCommentInfo_iCreateTime,
         |   a.stCommentInfo_iUpdateTime,
         |   a.stCommentInfo_sComment,
         |   a.vBroadcastTime,
         |   a.vStrategyId,
         |   a.updateTime
         |
         |   from ${TABLE_MANY} a
         |   left join  tempTable b
         |   on a.sKey=b.sKey
         |   where a.hp_stat_date=date_add(current_date(),${two})
         |   and b.updateTime is null
         |
         |    UNION ALL
         |
         | select * from tempTable
         |
      """.stripMargin

    //spark.sql(sqlStr).repartition(1).createOrReplaceTempView("tempTable2")
    //spark.sql(s"insert overwrite  table ${TABLE_MANY} partition(hp_stat_date='${timeStr}') select * from tempTable2")
    spark.sql(s"insert overwrite  table ${TABLE_MANY} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }

  def insertPortfolioToHiveGroupInfo(portData: RDD[PortGroupInfo], spark: SparkSession, dayFlag: Int): Unit = {
    val portRow = portData.map(line => {
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
    val createDataFrame = spark.createDataFrame(portRow, StructUtil.structPortGroupInfo).repartition(1).persist()
    createDataFrame.createOrReplaceTempView("tempTable")
    val two = dayFlag - 1
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    val sqlStr =
      s"""
         | select
         |   a.gi_iCreateTime,
         |   a.gi_isDel,
         |   a.gi_iUpdateTime,
         |   a.gi_sGroupName,
         |   a.gs_isDel,
         |   a.gs_iUpdateTime,
         |   a.gs_sDtSecCode,
         |   a.iVersion,
         |   a.sKey,
         |   a.updateTime
         | from ${TABLE_GROUP} a
         | left join  tempTable b
         | on a.sKey=b.sKey
         | where a.hp_stat_date=date_add(current_date(),${two})
         | and b.updateTime is null
         |
         | UNION ALL
         |
         | select * from tempTable
         |
      """.stripMargin
    //spark.sql(sqlStr).repartition(1).createOrReplaceTempView("tempTable2")
    //spark.sql(s"insert overwrite  table ${TABLE_GROUP} partition(hp_stat_date='${timeStr}') select * from tempTable2")
    spark.sql(s"insert overwrite  table ${TABLE_GROUP} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }

  def insertShareControl(shareRdds: RDD[ShareMany], spark: SparkSession) = {
    val shareRow = shareRdds.map(line => {
      import scala.collection.JavaConverters._
      var titleList = line.getRandomTitleConfigs().asScala
      if (titleList.isEmpty) {
        titleList = null
      }
      Row(
        line.getBgcolor,
        line.getBgUrl,
        line.getCode,
        line.getDesc,
        line.getDownShareViewUrl,
        line.getLink,
        line.getMb_content,
        line.getMb_contentLink,
        line.getMb_textColor,
        line.getMb_type,
        line.getMiddleChar,
        line.getName,
        line.getPermission,
        line.getPerson,
        line.getPosterChar,
        line.getRandomTitle,
        titleList,
        line.getSceneCode,
        line.getShareImgUrl,
        line.getShareTitle,
        line.getShareType,
        line.getTopBlockLink,
        line.getTopBlockUrl,
        line.getUpShareViewUrl
      )
    })
    val createDataFrame = spark.createDataFrame(shareRow, StructUtil.structShareMany)
    val reDF = createDataFrame.repartition(1).persist()
    reDF.createOrReplaceTempView("tempTable")
    spark.sql(s"insert overwrite  table ${TABLE_SHARE} select * from tempTable")
  }

  def insertEarlyWarn(userStockAlertCfgDataAllsRdd: RDD[UserStockAlertCfgDataAll],spark: SparkSession): Unit ={
    val userWarn = userStockAlertCfgDataAllsRdd.map(one => {


      Row(one.getiAccountId(),
        one.getvGUID(),
        one.getlUptTime(),
        one.getiSwitch(),
        one.isbLimitUp(),
        one.isbLimitDown(),
        one.isbSpeedUp(),
        one.isbSpeedDown(),
        one.isbChangeAsc(),
        one.isbChangeDesc(),
        one.isbDay30Highest(),
        one.isbDay60Highest(),
        one.getsDtSecCode(),
        one.getsDtSecName(),
        one.getdUpperPoint(),
        one.getdLowerPoint(),
        one.getdDayChangeAsc(),
        one.getdDayChangeDesc(),
        one.getdFiveMinChangeAsc(),
        one.getdFiveMinChangeDesc(),
        one.getvBroadcastTime() match {
          case null => null
          case _ => JavaConversions.asScalaBuffer(one.getvBroadcastTime())
        },
        one.getvStrategyId() match {
          case null=> null
          case _=>JavaConversions.asScalaBuffer(one.getvStrategyId())
        },
        one.getDataKey,
        one.getUpdateTime)
    })
    val createDataFrame = spark.createDataFrame(userWarn, StructUtil.structEarlyWarn).repartition(1)
    createDataFrame.createOrReplaceTempView("tempTable")
    spark.sql(s"insert overwrite  table ${TABLE_WARN} select * from tempTable")

  }



}
