package sparkAction

import java.util

import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{DateScalaUtil, LocalOrLine, MailUtil}
import sparkAction.StringIpActionListHive.{BuryClientWebTableStringIp, BuryDeviceInfosStringIp, BuryPhoneClientTableStringIp, BuryVisitTableStringIp}
import sparkAction.buryCleanUtil.BuryCleanCommon
import sparkAction.mapIpActionListHive._

/**
  * Created by lenovo on 2018/11/16.
  * 该类是埋点日志清洗入库
  * 主要有两版日志
  */
object BuryMainFunction {
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")
  private val dict: String = ConfigurationManager.getProperty("stock.web.dict")

  def main(args: Array[String]): Unit = {
    try {
      val diffDay: Int = args(0).toInt
      val local: Boolean = LocalOrLine.judgeLocal()

      var sparkConf: SparkConf = new SparkConf().setAppName("BuryMainFunction")
      if (local) {
        //System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      /*
      暂停使用
      val sc: SparkContext = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")*/
      val spark = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
      //for (dayFlag <- (diffDay to -1)) { //按天数循环
        val realPath = hdfsPath + DateScalaUtil.getPreviousDateStr(diffDay, 2)
        //val realPath="E:\\desk\\日志out\\rzout"
        val file: RDD[String] = spark.sparkContext.textFile(realPath, 1)
        val dictRdd = spark.sparkContext.textFile(dict).collect()
        val dictBrod = spark.sparkContext.broadcast(dictRdd).value
        val filterBlank: RDD[String] = file.filter(line => {
          //过滤为空的和有ip但是post传递为空的
          StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
        })
        //清洗去掉不规则字符
        val allData = filterBlank.map(BuryCleanCommon.cleanCommonToListBuryLogin).filter(_.size() > 0)
        val rddOneObjcet: RDD[AnyRef] = allData.flatMap(_.toArray())
        val allDataOneRdd = rddOneObjcet.map(_.asInstanceOf[BuryLogin]).filter(one => {
          val line = one.line
          StringUtils.isNotBlank(line)
        })
        val oldDataOneRdd: RDD[BuryLogin] = allDataOneRdd.filter(BuryCleanCommon.getOldVersionFunction)
        //老规则数据清洗入库
        oldVersionCleanInsert(oldDataOneRdd, spark, diffDay)
        //新规则数据+老规则数据清洗入库
        newVersionCleanInsert(allDataOneRdd, spark, diffDay, dictBrod)
        //测试
        //testFun2(allDataOneRdd,spark,0,dictBrod)
     // }
      spark.close()
    } catch {
      case e: Throwable => e.printStackTrace();MailUtil.sendMailNew("spark日志清洗调度","清洗失败")
    }
  }

  //清洗老日志
  def oldVersionCleanInsert(oldDataRddList: RDD[BuryLogin], spark: SparkSession, dayFlag: Int): Unit = {
    //过滤出pc端web日志
    val pcWebRdd = oldDataRddList.filter(BuryCleanCommon.getPcWebLog)
    //清洗出pc端web日志
    BuryPcWebTableMapIp.cleanPcWebData(pcWebRdd, spark, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    //过滤出手机web日志
    val filterPhoneWeb = oldDataRddList.filter(BuryCleanCommon.getPhoneWebLog) //手机web
    //清洗出手机web端的日志
    BuryPhoneWebTableMapIp.cleanPhoneWebData(filterPhoneWeb, spark, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    //val filterVisit = oldDataRddList.filter(_.logType == 1) //过滤出访问日志Data
    //清洗出股掌柜手机客户端访问日志数据
    //BuryVisitTableMapIp.cleanVisitData(filterVisit, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    val filterAction = oldDataRddList.filter(_.logType == 2) //过滤出行为日志Data
    //过滤出客户端行为日志
    //val filterClient = filterAction.filter(BuryCleanCommon.getPhoneClientActionLog)
    //清洗出股掌柜手机客户端行为数据
    //BuryPhoneClientTableMapIp.cleanPhoneClientData(filterClient, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    //过滤出股掌柜pc客户端行为数据
    val pcClient = filterAction.filter(_.source == 4)
    //清洗出股掌柜pc客户端的数据
    BuryPcClientTableMapIp.cleanPcClientData(pcClient, spark, dayFlag)
    //-------------------------------------------------------------------------------------------------------
    //过滤出手机客户端内嵌网页端行为日志
    //val filterWeb = filterAction.filter(_.source==3)
    //清洗股掌柜手机客户端内嵌网页行为数据
    //BuryClientWebTableMapIp.cleanClientWebData(filterWeb, hc, dayFlag)
  }

  //新+旧(日志)一起清洗
  def newVersionCleanInsert(DataRddList: RDD[BuryLogin], spark: SparkSession, dayFlag: Int, dictBrod: Array[String]) = {
    //过滤出pc端web日志
    //val pcWebRdd = DataRddList.filter(BuryCleanCommon.getPcWebLog)
    //清洗出pc端web日志
    //BuryPcWebTableStringIp.cleanPcWebData(pcWebRdd, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    //过滤出手机web日志
    //val filterPhoneWeb = DataRddList.filter(BuryCleanCommon.getPhoneWebLog) //手机web
    //清洗出手机web端的日志
    //BuryPhoneWebTableStringIp.cleanPhoneWebData(filterPhoneWeb, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    val filterVisit = DataRddList.filter(_.logType == 1) //过滤出访问日志Data
    //清洗出股掌柜手机客户端访问日志数据
    BuryVisitTableStringIp.cleanVisitData(filterVisit, spark, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    val filterAction = DataRddList.filter(_.logType == 2) //过滤出行为日志Data
    //过滤出客户端行为日志
    val filterClient = filterAction.filter(BuryCleanCommon.getPhoneClientActionLog)
    //清洗出股掌柜手机客户端行为数据
    BuryPhoneClientTableStringIp.cleanPhoneClientData(filterClient, spark, dayFlag)
    //-------------------------------------------------------------------------------------------------------
    val filterDeviceInfos = DataRddList.filter(_.logType==3)
    BuryDeviceInfosStringIp.cleanBuryDeviceInfosStringIp(filterDeviceInfos,spark,dayFlag)
    //val pcClient = filterAction.filter(_.source == 4)
    //清洗出股掌柜pc客户端的数据
    //BuryPcClientTableMapIp.cleanPcClientData(pcClient, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------
    //过滤出手机客户端内嵌网页端行为日志
    val filterWeb = filterAction.filter(_.source == 3)
    //清洗股掌柜手机客户端内嵌网页行为数据
    BuryClientWebTableStringIp.cleanClientWebData(filterWeb, spark, dayFlag, dictBrod)
  }

  def testFun2(DataRddList: RDD[BuryLogin], spark: SparkSession, dayFlag: Int, dictBrod: Array[String]): Unit = {
    val filterAction = DataRddList.filter(_.logType == 2) //过滤出行为日志Data
    val filterWeb = filterAction.filter(_.source == 3)
    BuryClientWebTableStringIp.cleanClientWebData2(filterWeb, spark, dayFlag, dictBrod)
  }
}

case class BuryLogin(var line: String, var sendTime: String, var source: Int, var logType: Int, var ipStr: String)


