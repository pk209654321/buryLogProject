package testBury

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scalaUtil.LocalOrLine
import sparkAction.BuryLogin
import sparkAction.BuryMainFunction.cleanCommonFunction

/**
  * ClassName TempRunBury
  * Description TODO
  * Author lenovo
  * Date 2018/12/28 9:01
  **/
object TempRunBury {
  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()

    var sparkConf: SparkConf = new SparkConf().setAppName("BuryMainFunction")
    if (local) {
      System.setProperty("HADOOP_USER_NAME", "wangyd")
      sparkConf = sparkConf.setMaster("local[*]")
    }
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val hc: HiveContext = new HiveContext(sc)
    val realPath =""
    val outPath=""
    val file: RDD[String] = sc.textFile(realPath, 1)
    val filterBlank: RDD[String] = file.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    val map: RDD[BuryLogin] = filterBlank.map(cleanCommonFunction).cache()
    val tempRdd = map.filter(_.line.indexOf("")>0)
    tempRdd.saveAsTextFile("")
  }
}
