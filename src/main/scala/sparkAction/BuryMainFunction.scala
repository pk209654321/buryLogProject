package sparkAction

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
  def selectStockField(dict: Array[String],key:String): Boolean ={
    if(StringUtils.isBlank(key)){
      return false
    }
    var flag:Boolean=false
    for (elem <- dict if !flag) {
      if (elem==key){//在字典表中找到了改key
        flag=true
      }
    }
    flag
  }
  //清洗原始日志
  val cleanCommonFunction = (line: String) => {
    val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
    val jsonAndIp: Array[String] = all.split("&")
    if (jsonAndIp.length >= 2) {
      //如果有ip和 httpurl 代'&'的埋点类容
      val i = all.lastIndexOf("&")
      val bury = all.substring(0, i)
      val ipTemp = all.substring(i + 1, all.length)
      var buryLogin=BuryLogin("","",0,0,"")
      try {
        buryLogin = JSON.parseObject(bury, classOf[BuryLogin])
      } catch {
        case e => println(s"error_log:${all}")
      }
      buryLogin.ipStr = ipTemp
      buryLogin
    } else {
      try {
        JSON.parseObject(all, classOf[BuryLogin])
      } catch {
        case e => MailUtil.sendMail("spark日志清洗调度","发现错误日志:"+all); BuryLogin("","",0,0,"")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      val diffDay: Int = args(0).toInt
      val local: Boolean = LocalOrLine.judgeLocal()

      var sparkConf: SparkConf = new SparkConf().setAppName("BuryMainFunction")
      if (local) {
        System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val sc: SparkContext = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      val hc: HiveContext = new HiveContext(sc)
      for (dayFlag <- (diffDay to -1)) { //按天数循环
      val realPath = hdfsPath + DateScalaUtil.getPreviousDateStr(dayFlag, 2)
      //val realPath=hdfsPath
      val file: RDD[String] = sc.textFile(realPath, 1)
      val dictRdd = sc.textFile(dict).collect()
      val dictBrod = sc.broadcast(dictRdd).value

      val filterBlank: RDD[String] = file.filter(line => {
        //过滤为空的和有ip但是post传递为空的
        StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
      })
      //清洗去掉不规则字符
      val map: RDD[BuryLogin] = filterBlank.map(cleanCommonFunction).cache()
      //过滤出pc端web日志
      val pcWebRdd: RDD[BuryLogin] = map.filter(one => {
        val line = one.line
        val i = line.indexOf("application=browser")
        if (i > 0) {
          true
        } else {
          false
        }
      })
      //清洗出pc端web日志
      BuryPcWebTableMapIp.cleanPcWebData(pcWebRdd, hc, dayFlag)
      //过滤出手机web日志
      val filterPhoneWeb: RDD[BuryLogin] = map.filter(one => {
        val line = one.line
        val i = line.indexOf("application=web")
        if (i >= 0) {
          true
        } else {
          false
        }
      }) //手机web
      //清洗出手机web端的日志
      BuryPhoneWebTableMapIp.cleanPhoneWebData(filterPhoneWeb, hc, dayFlag)
      val filterVisit: RDD[BuryLogin] = map.filter(_.logType == 1) //过滤出访问日志Data
      //清洗出股掌柜手机客户端访问日志数据
      BuryVisitTableMapIp.cleanVisitData(filterVisit, hc, dayFlag)
      //=========================================================================================
      val filterAction: RDD[BuryLogin] = map.filter(_.logType == 2) //过滤出行为日志Data
      val filterClient: RDD[BuryLogin] = filterAction.filter(line => {
        val source: Int = line.source
        if (source == 1 || source == 2) {
          //安卓和ios端
          //过滤出手机客户端Data
          true
        } else {
          false
        }
      })
      val pcClient: RDD[BuryLogin] = filterAction.filter(_.source == 4)
      //清洗出股掌柜手机客户端行为数据
      BuryPhoneClientTableMapIp.cleanPhoneClientData(filterClient, hc, dayFlag)
      //清洗出股掌柜pc客户端的数据
      BuryPcClientTableMapIp.cleanPcClientData(pcClient, hc, dayFlag)
      val filterWeb: RDD[BuryLogin] = filterAction.filter(line => {
        val source: Int = line.source
        if (source == 3) {
          //过滤出网页端数据k
          true
        } else {
          false
        }
      })
      //清洗股掌柜手机客户端内嵌网页行为数据
      BuryClientWebTableMapIp.cleanClientWebData(filterWeb, hc, dayFlag)
      //BuryClientWebTableStringIpMap.cleanClientWebData(filterWeb,hc,dayFlag,dictBrod)
      }
      sc.stop()
    } catch {
      case e => MailUtil.sendMail("spark日志清洗调度","清洗日志失败:"+e.getMessage)
    }
  }
}

case class BuryLogin(var line: String, var sendTime: String, var source: Int, var logType: Int, var ipStr: String)


