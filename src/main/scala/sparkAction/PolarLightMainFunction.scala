package sparkAction

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import scalaUtil.{DateScalaUtil, LocalOrLine, MailUtil}
import sparkAction.polarLightForAction.{PolarLightAction, PolarLightActionNew}

/**
  * ClassName PolarLightFunction
  * Description TODO
  * Author lenovo
  * Date 2019/5/17 9:43
  * 极光效果,调用接口推送其数据
  **/
object PolarLightMainFunction {
  def main(args: Array[String]): Unit = {
    try {
      //val local: Boolean = LocalOrLine.judgeLocal()

      var sparkConf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      //sparkConf.set("spark.network.timeout", "3600")
      sparkConf.set("spark.sql.shuffle.partitions", "20")
      sparkConf.set("spark.default.parallelism", "20")
      if (LocalOrLine.isWindows) {
        sparkConf.setMaster("local[*]")
        println("----------------------------开发模式")
        //return
      }
      val spark = SparkSession.builder().
        config(sparkConf).
        enableHiveSupport().
        getOrCreate()
      //      PolarLightAction.polarLightForActive(spark)
      //      PolarLightAction.polarLightForRegister(spark)

      //新版
      PolarLightActionNew.polarLightForActive(spark)
      PolarLightActionNew.polarLightForRegister(spark)
      spark.close()
    } catch {
      case e: Throwable => e.printStackTrace()
      //MailUtil.sendMailNew("极光效果推送警报", "推送失败")
    }
  }

}
