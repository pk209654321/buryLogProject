package sparkAction

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import scalaUtil.{DateScalaUtil, LocalOrLine, MailUtil}
import sparkAction.polarLightForAction.PolarLightAction

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
      val local: Boolean = LocalOrLine.judgeLocal()

      var sparkConf: SparkConf = new SparkConf().setAppName("PolarLightFunction")
      if (local) {
        //System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val spark = SparkSession.builder().
        config(sparkConf).
        enableHiveSupport().
        getOrCreate()
      PolarLightAction.polarLightForActive(spark)
      PolarLightAction.polarLightForRegister(spark)
      spark.close()
    } catch {
      case e: Throwable => e.printStackTrace();MailUtil.sendMail("极光效果推送警报", e.toString)
    }
  }

}
