package testBury

import java.util.Date

import com.alibaba.fastjson.JSON
import hadoopCode.hbaseCommon.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scalaUtil.{DateScalaUtil, LocalOrLine, MailUtil}
import sparkAction.{BuryLogin, BuryMainFunction}
import sparkAction.BuryMainFunction.cleanCommonFunction
import sparkAction.mapIpAction.BuryClientWebTableMapIp

/**
  * ClassName TempRunBury
  * Description TODO
  * Author lenovo
  * Date 2018/12/28 9:01
  **/
object TempRunBury {

  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()
    //获取当前类的名称
    val className = this.getClass.getSimpleName
    var sparkConf: SparkConf = new SparkConf().setAppName(s"${className}")
    if (local) {
      System.setProperty("HADOOP_USER_NAME", "wangyd")
      sparkConf = sparkConf.setMaster("local[1]")
    }
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val hc: HiveContext = new HiveContext(sc)
    //val hc: HiveContext = new HiveContext(sc)
    val realPath ="E:\\desk\\日志"
    val file: RDD[String] = sc.textFile(realPath, 1)
    val filterBlank: RDD[String] = file.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    val unit = filterBlank.map(BuryMainFunction.cleanCommonFunction)
    val filterAction: RDD[BuryLogin] = unit.filter(_.logType == 2) //过滤出行为日志Data
    val filterWeb: RDD[BuryLogin] = filterAction.filter(line => {
      val source: Int = line.source
      if (source == 3) {
        //过滤出网页端数据k
        true
      } else {
        false
      }
    })
    BuryClientWebTableMapIp.cleanClientWebData(filterWeb, hc, 0)





  }
}
