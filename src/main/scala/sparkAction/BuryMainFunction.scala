package sparkAction

import java.util

import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{DateScalaUtil, LocalOrLine, MailUtil}
import sparkAction.StringIpMapAction.BuryClientWebTableStringIpMap
import sparkAction.mapIpAction._

/**
  * Created by lenovo on 2018/11/16.
  * 将埋点数据清洗,形成两张表
  */
object BuryMainFunction {
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")
  private val dict: String = ConfigurationManager.getProperty("stock.web.dict")


  //查询股市app_web字段,将其他字段放入other_map中
  def selectStockField(dict: Array[String], key: String): Boolean = {
    if (StringUtils.isBlank(key)) {
      return false
    }
    var flag: Boolean = false
    for (elem <- dict if !flag) {
      if (elem == key) {
        //在字典表中找到了改key
        flag = true
      }
    }
    flag
  }

  //获取老版本的日志
  val getOldVersionFunction = (list: util.List[BuryLogin]) => {
    val line = list.get(0).line
    val strings = line.split("\\|")
    val i = strings(0).indexOf("=")
    i >= 0
  }

  //获取PC端web日志
  val getPcWebLog = (list: util.List[BuryLogin]) => {
    val line = list.get(0).line
    val i = line.indexOf("application=browser")
    if (i > 0) {
      true
    } else {
      false
    }
  }
  //获取手机web日志
  val getPhoneWebLog = (list: util.List[BuryLogin]) => {
    val line = list.get(0).line
    val i = line.indexOf("application=web")
    if (i >= 0) {
      true
    } else {
      false
    }
  }
  //获取客户端行为日志
  val getPhoneClientActionLog=(list: util.List[BuryLogin])=>{
    val source: Int = list.get(0).source
    if (source == 1 || source == 2) {
      //安卓和ios端
      //过滤出手机客户端Data
      true
    } else {
      false
    }
  }
  //清洗原始日志
  val cleanCommonFunction: String => util.List[BuryLogin] = (line: String) => {
    val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
    val jsonAndIp: Array[String] = all.split("&")
    var listbury: java.util.List[BuryLogin] = new util.ArrayList[BuryLogin]()
    if (jsonAndIp.length >= 2) {
      //如果有ip和 httpurl 代'&'的埋点类容
      val ifList = all.indexOf("[")
      //判断是否是打包上传
      var bury = ""
      val i = all.lastIndexOf("&")
      val ipTemp = all.substring(i + 1, all.length)
      if (ifList == 0) { //如果==0 是打包上传传递的是jsonList格式的日志
        bury = all.substring(0, i)
      } else {
        bury = "[" + all.substring(0, i) + "]"
      }
      try {
        listbury = JSON.parseArray(bury, classOf[BuryLogin])
      } catch {
        case e: Throwable => println(s"error_log:${all}")
      }
      for (i <- (0 to listbury.size() - 1)) {
        val login = listbury.get(i)
        login.ipStr = ipTemp
      }
      listbury
    } else {
      try {
        JSON.parseArray("[" + all + "]", classOf[BuryLogin])
      } catch {
        case e: Throwable => MailUtil.sendMail("spark日志清洗调度", "发现错误日志:" + all); listbury
      }
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      val diffDay: Int = args(0).toInt
      val local: Boolean = LocalOrLine.judgeLocal()

      var sparkConf: SparkConf = new SparkConf().setAppName("BuryMainFunction")
      if (local) {
        //System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val sc: SparkContext = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      val hc: HiveContext = new HiveContext(sc)
      for (dayFlag <- (diffDay to -1)) { //按天数循环
        val realPath = hdfsPath + DateScalaUtil.getPreviousDateStr(dayFlag, 2)
        //val realPath=hdfsPath
        val file: RDD[String] = sc.textFile(realPath, 1)
        //val dictRdd = sc.textFile(dict).collect()
        //val dictBrod = sc.broadcast(dictRdd).value
        val filterBlank: RDD[String] = file.filter(line => {
          //过滤为空的和有ip但是post传递为空的
          StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
        })
        //清洗去掉不规则字符
        val allData = filterBlank.map(cleanCommonFunction).filter(_.size()>0).cache()
        val oldDataRddList: RDD[util.List[BuryLogin]] = allData.filter(getOldVersionFunction)
        //过滤出pc端web日志
        val pcWebRdd = oldDataRddList.filter(getPcWebLog)
        //清洗出pc端web日志
        //BuryPcWebTableMapIp.cleanPcWebData(pcWebRdd, hc, dayFlag)
        //-------------------------------------------------------------------------------------------------------

        //过滤出手机web日志
        val filterPhoneWeb = oldDataRddList.filter(getPhoneWebLog) //手机web
        //清洗出手机web端的日志
        //BuryPhoneWebTableMapIp.cleanPhoneWebData(filterPhoneWeb, hc, dayFlag)
        //-------------------------------------------------------------------------------------------------------

        val filterVisit = oldDataRddList.filter(_.get(0).logType == 1) //过滤出访问日志Data
        //清洗出股掌柜手机客户端访问日志数据
        //BuryVisitTableMapIp.cleanVisitData(filterVisit, hc, dayFlag)
        //-------------------------------------------------------------------------------------------------------

        val filterAction = oldDataRddList.filter(_.get(0).logType == 2) //过滤出行为日志Data
        //过滤出客户端行为日志
        val filterClient = filterAction.filter(getPhoneClientActionLog)
        //清洗出股掌柜手机客户端行为数据
        //BuryPhoneClientTableMapIp.cleanPhoneClientData(filterClient, hc, dayFlag)
        //-------------------------------------------------------------------------------------------------------

        val pcClient = filterAction.filter(_.get(0).source == 4)
        //清洗出股掌柜pc客户端的数据
        //BuryPcClientTableMapIp.cleanPcClientData(pcClient, hc, dayFlag)
        //-------------------------------------------------------------------------------------------------------
        //过滤出手机客户端内嵌网页端行为日志
        val filterWeb = filterAction.filter(getPhoneClientActionLog)
        //清洗股掌柜手机客户端内嵌网页行为数据
        //BuryClientWebTableMapIp.cleanClientWebData(filterWeb, hc, dayFlag)
        //BuryClientWebTableStringIpMap.cleanClientWebData(filterWeb,hc,dayFlag,dictBrod)
      }
      sc.stop()
    } catch {
      case e: Throwable => MailUtil.sendMail("spark日志清洗调度", "清洗日志失败:" + e.printStackTrace())
    }
  }

  def oldVersionCleanInsert(oldDataRddList: RDD[util.List[BuryLogin]],hc: HiveContext,dayFlag:Int): Unit = {
    //过滤出pc端web日志
    val pcWebRdd = oldDataRddList.filter(getPcWebLog)
    //清洗出pc端web日志
    BuryPcWebTableMapIp.cleanPcWebData(pcWebRdd, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    //过滤出手机web日志
    val filterPhoneWeb = oldDataRddList.filter(getPhoneWebLog) //手机web
    //清洗出手机web端的日志
    BuryPhoneWebTableMapIp.cleanPhoneWebData(filterPhoneWeb, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    val filterVisit = oldDataRddList.filter(_.get(0).logType == 1) //过滤出访问日志Data
    //清洗出股掌柜手机客户端访问日志数据
    BuryVisitTableMapIp.cleanVisitData(filterVisit, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    val filterAction = oldDataRddList.filter(_.get(0).logType == 2) //过滤出行为日志Data
    //过滤出客户端行为日志
    val filterClient = filterAction.filter(getPhoneClientActionLog)
    //清洗出股掌柜手机客户端行为数据
    BuryPhoneClientTableMapIp.cleanPhoneClientData(filterClient, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------

    val pcClient = filterAction.filter(_.get(0).source == 4)
    //清洗出股掌柜pc客户端的数据
    BuryPcClientTableMapIp.cleanPcClientData(pcClient, hc, dayFlag)
    //-------------------------------------------------------------------------------------------------------
    //过滤出手机客户端内嵌网页端行为日志
    val filterWeb = filterAction.filter(getPhoneClientActionLog)
    //清洗股掌柜手机客户端内嵌网页行为数据
    BuryClientWebTableMapIp.cleanClientWebData(filterWeb, hc, dayFlag)
  }

  def newVersionCleanInsert(oldDataRddList: RDD[util.List[BuryLogin]],hc: HiveContext,dayFlag:Int) = {

  }
}

case class BuryLogin(var line: String, var sendTime: String, var source: Int, var logType: Int, var ipStr: String)


