package sparkAction

import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scalaUtil.{LocalOrLine, DateScalaUtil}

/**
  * Created by lenovo on 2018/11/16.
  * 将埋点数据清洗,形成两张表
  */
object BuryMainFunction {
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")

  def main(args: Array[String]): Unit = {
    val diffDay: Int = args(0).toInt
    val local: Boolean = LocalOrLine.judgeLocal()
    var sparkConf: SparkConf = new SparkConf().setAppName("BuryTwoTable")
    if (local) {
      System.setProperty("HADOOP_USER_NAME", "wangyd")
      sparkConf = sparkConf.setMaster("local[*]")
    }
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //val sqlConf: SQLContext = new SQLContext(sc)
    val hc: HiveContext = new HiveContext(sc)
    //val file: RDD[String] = sc.textFile("C:\\Users\\lenovo\\Desktop\\bushu\\access.log",4)
    val realPath = hdfsPath + DateScalaUtil.getPreviousDateStr(diffDay, 2)
    val file: RDD[String] = sc.textFile(realPath,1)
    val filterBlank: RDD[String] = file.filter(line => {
      StringUtils.isNotBlank(line)
    })
    val map: RDD[BuryLogin] = filterBlank.map(line => {
      //替换字符串
      val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
      val jsonAndIp: Array[String] = all.split("&")
      jsonAndIp
      JSON.parseObject(all, classOf[BuryLogin])
    }).cache()


    val filterVisit: RDD[BuryLogin] = map.filter(_.logType == 1) //过滤出访问日志Data
    //清洗出访问日志数据
     //BuryVisitTable.cleanVisitData(filterVisit,hc,diffDay)
    //=========================================================================================
    val filterAction: RDD[BuryLogin] = map.filter(_.logType == 2) //过滤出行为日志Data
    val filterClient: RDD[BuryLogin] = filterAction.filter(line => {
        val source: Int = line.source
        if (source == 1 || source == 2) {
          //过滤出客户端Data
          true
        } else {
          false
        }
      })
    //清洗出客户端数据行为数据
    //BuryClientTable.cleanClientData(filterClient, hc, diffDay)
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
    //BuryWebTable.cleanWebData(filterWeb,hc,diffDay)

    //用户上报
    //BuryLoginReport.repotUserLogin(filterVisit)
    sc.stop()
  }
}

case class BuryLogin(var line: String,var sendTime: String,var source: Int,var logType: Int,var ipStr:String)


