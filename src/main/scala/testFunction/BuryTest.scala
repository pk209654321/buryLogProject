package testFunction

import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{DateScalaUtil, LocalOrLine}
import sparkAction.mapAction.{BuryClientTableMap, BuryPhoneWebTableMap, BuryVisitTableMap, BuryWebTableMap}

/**
  * Created by lenovo on 2018/11/16.
  * 将埋点数据清洗,形成两张表
  */
object BuryTest {
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")

  def test(str:String):String={
    /**
    　　* @Description: TODO
    　　* @param [str]
    　　* @return java.lang.String
    　　* @throws
    　　* @author lenovo
    　　* @date 2018/12/4 14:45
    　　*/

    ""
  }
  def main(args: Array[String]): Unit = {
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
    //val realPath = hdfsPath + DateScalaUtil.getPreviousDateStr(diffDay, 2)
    val file: RDD[String] = sc.textFile("E:\\desk\\日志\\bury", 1)
    val filterBlank: RDD[String] = file.filter(line => {//过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    val map: RDD[BuryLogin] = filterBlank.map(line => {
      //替换字符串
      val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
      val jsonAndIp: Array[String] = all.split("&")
      if (jsonAndIp.length >= 2) {//如果有ip和 httpurl 代'&'的埋点类容
        val i = all.lastIndexOf("&")
        val bury = all.substring(0,i)
        val ipTemp = all.substring(i+1,all.length)
        val buryLogin: BuryLogin = JSON.parseObject(bury, classOf[BuryLogin])
        buryLogin.ipStr = ipTemp
        buryLogin
      } else {
          JSON.parseObject(all, classOf[BuryLogin])
      }

    }).cache()
    //val filterPhoneWeb: RDD[BuryLogin] = map.filter(_.source==5)//手机web
    //清洗出手机web端的日志
    //BuryPhoneWebTableMap.cleanPhoneWebData(filterPhoneWeb,hc,diffDay)
    val filterVisit: RDD[BuryLogin] = map.filter(_.logType == 1) //过滤出访问日志Data

    filterVisit.map(one => {
      val line = one.line
      val strings: Array[String] = line.split("\\|")

      for (elem <- strings) {
        var trimKey=""
        var trimValue=""
        if (StringUtils.isNotBlank(elem)){
          val strings: Array[String] = elem.split("=")
          if(strings.length>=2){
            trimKey = strings(0).trim
            trimValue = strings(1).trim
          }
        }
        trimKey match {
          case "user_id" => if (trimValue=="37894") println(line)
          case _ =>
        }
        /*trimKey match {
          //case "phone_model" => if(trimValue.indexOf("iPhone")>0) println(line)
        }*/
      }
    }).collect()


    //清洗出访问日志数据
    //=========================================================================================
    val filterAction: RDD[BuryLogin] = map.filter(_.logType == 2) //过滤出行为日志Data
    val filterClient: RDD[BuryLogin] = filterAction.filter(line => {
      val source: Int = line.source
      if (source == 1 || source == 2|| source==4) {
        //过滤出客户端Data
        true
      } else {
        false
      }
    })
    //清洗出客户端数据行为数据
    val filterWeb: RDD[BuryLogin] = filterAction.filter(line => {
      val source: Int = line.source
      if (source == 3) {
        //过滤出网页端数据k
        true
      } else {
        false
      }
    })
    //清洗出网页端行为数据

    sc.stop()
  }
}

case class BuryLogin(var line: String, var sendTime: String, var source: Int, var logType: Int, var ipStr: String)


