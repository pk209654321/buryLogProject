package localAction

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import scalaUtil.{DateScalaUtil, LocalOrLine}
import sparkAction.BuryMainFunction
import testFunction.BuryTest1
import testFunction.BuryTest1.hdfsPath

/**
  * ClassName LocalScala
  * Description TODO
  * Author lenovo
  * Date 2018/12/27 17:53
  **/
object LocalScala {
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
    //输入路径
    val realPath = args(0)
    val file: RDD[String] = sc.textFile(realPath, 1)
    val filterBlank: RDD[String] = file.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    val value= filterBlank.map(BuryMainFunction.cleanCommonFunction)
    //输出路径
    value.saveAsTextFile(args(1))
  }

}
