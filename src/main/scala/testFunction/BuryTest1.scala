package testFunction

import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{DateScalaUtil, LocalOrLine}
import sparkAction.BuryMainFunction
import sparkAction.mapAction.{BuryClientTableMap, BuryPhoneWebTableMap, BuryVisitTableMap, BuryWebTableMap}

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
    val value: RDD[sparkAction.BuryLogin] = filterBlank.map(BuryMainFunction.cleanCommonFunction)
    //val filterPhoneWeb: RDD[BuryLogin] = map.filter(_.source==5)//手机web
    //清洗出手机web端的日志
    //BuryPhoneWebTableMap.cleanPhoneWebData(filterPhoneWeb,hc,diffDay)
    val filterVisit: RDD[sparkAction.BuryLogin] = value.filter(_.source == 4)

    val fv = filterVisit.filter(one => {
      val line = one.line
      val i = line.indexOf("application=web")
      if (i >= 0) {
        true
      } else {
        false
      }
    })
    BuryPhoneWebTableMap.cleanPhoneWebData(fv, hc, diffDay)
  }


}


