package sparkAction

import conf.ConfigurationManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalaUtil.{LocalOrLine, MailUtil}
import sparkAction.TagInfoForAction.TagInfoAction

/**
  * ClassName TagInfoMainFunction
  * Description TODO
  * Author lenovo
  * Date 2019/5/31 10:35
  **/
object TagInfoMainFunction {
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")
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

      //select user_id from cdm.t_user_tags where ${sqlWhere
      TagInfoAction.doTagInfoAction2(spark,"tag_id='VIP_20190409105556'")
      spark.close()
    } catch {
      case e: Throwable => e.printStackTrace();//MailUtil.sendMail("极光效果推送警报", e.toString)
    }
  }
}
