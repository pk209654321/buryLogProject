package testFunction

import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{DateScalaUtil, LocalOrLine}

/**
  * Created by lenovo on 2018/11/16.
  * 将埋点数据清洗,形成两张表
  */
object BuryTest1 {
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")

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
    val realPath = hdfsPath + DateScalaUtil.getPreviousDateStr(diffDay, 2)
    val file: RDD[String] = sc.textFile(realPath, 1)
    val filterBlank: RDD[String] = file.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    val value: RDD[BuryTest1] = filterBlank.map(line => {
      val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
      val jsonAndIp: Array[String] = all.split("&")
      if (jsonAndIp.length >= 2) {
        //如果有ip和 httpurl 代'&'的埋点类容
        val i = all.lastIndexOf("&")
        val bury = all.substring(0, i)
        val ipTemp = all.substring(i + 1, all.length)
        var buryLogin=BuryTest1("","",0,0,"")
        try {
           buryLogin = JSON.parseObject(bury, classOf[BuryTest1])
        } catch{
          case e => println(all)
        }
        buryLogin.ipStr = ipTemp
        buryLogin
      } else {
        JSON.parseObject(all, classOf[BuryTest1])
      }
    })

    value.collect()
  }
}


case class BuryTest1(var line: String, var sendTime: String, var source: Int, var logType: Int, var ipStr: String)

