package sparkAction

import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{DateScalaUtil, LocalOrLine, MailUtil}
import sparkAction.buryCleanUtil.BuryCleanCommon
import sparkAction.mapAction.BuryLoginReportNew

/**
  * Created by lenovo on 2018/11/16.
  * 用户登录上报入口程序
  */
object BuryMainUser {
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")

  def main(args: Array[String]): Unit = {
    try {
      val diffDay: Int = args(0).toInt
      val local: Boolean = LocalOrLine.judgeLocal()
      var sparkConf: SparkConf = new SparkConf().setAppName("BuryMainUser")
      if (local) {
        System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val sc: SparkContext = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      val hc: HiveContext = new HiveContext(sc)
      //val realPath = hdfsPath + DateScalaUtil.getPreviousDateStr(diffDay, 2)
      val realPath = "E:\\desk\\新版本日志"
      val file: RDD[String] = sc.textFile(realPath, 1)
      val filterBlank: RDD[String] = file.filter(line => {
        StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
      })
      //清洗数据
      val allData = filterBlank.map(BuryCleanCommon.cleanCommonFunction).filter(_.size() > 0)
      val rddOneObjcet: RDD[AnyRef] = allData.flatMap(_.toArray())
      val allDataOneRdd = rddOneObjcet.map(_.asInstanceOf[BuryLogin]).cache()
      val filterVisit: RDD[BuryLogin] = allDataOneRdd.filter(_.logType == 1) //过滤出访问日志Data
      val oldDataOneRdd: RDD[BuryLogin] = filterVisit.filter(BuryCleanCommon.getOldVersionFunction)
      val newDataOneRdd = filterVisit.filter(BuryCleanCommon.getNewVersionFunction)
      //旧版本数据用户启动上报
      //BuryLoginReportNew.repotUserLogin(oldDataOneRdd)
      //新版本数据
      BuryLoginReportNew.repotUserLoginNew(newDataOneRdd)
      sc.stop()
    } catch {
      case e: Throwable => MailUtil.sendMail("spark用户登录上报", "用户登录上报错误")
    }
  }
}



