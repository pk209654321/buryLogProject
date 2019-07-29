package sparkAction

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalaUtil.{LocalOrLine, MailUtil}
import sparkAction.portfolioHive.NfRiskAssessmentUserCommitRecordToHive

import scala.collection.mutable.ListBuffer


/**
  * ClassName PortfolioMainFunction
  * Description TODO 解析答题数据
  * Author lenovo
  * Date 2019/2/12 9:16
  **/

object PushDataObject {
  def main(args: Array[String]): Unit = {
    try {
      val local: Boolean = LocalOrLine.judgeLocal()
      var sparkConf: SparkConf = new SparkConf().setAppName("PushDataObject")
      if (local) {
        System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      //val sc: SparkContext = new SparkContext(sparkConf)
      val spark = SparkSession.builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
      import spark.implicits._
      val dataFrame = spark.sql("select msg_id,transmission_content from  db_message.gexin_task_message_relation")
      val ansData = dataFrame.as[MsgContentData]
      //NfRiskAssessmentUserCommitRecordToHive.insertAnswerNew(ansData,spark)
      spark.close()
    } catch {
      case e: Throwable => e.printStackTrace(); MailUtil.sendMailNew("spark答题数据", "解析失败-----"+e.getMessage)
    }
  }
}

case class MsgContentData(msg_id:String,transmission_content:String)

