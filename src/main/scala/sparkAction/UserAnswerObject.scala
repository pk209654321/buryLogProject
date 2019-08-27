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

case class AnswersBean(ID: Long, ACCOUNT_ID: String, USER_ANS: String, UPDATE_TIME: java.lang.Long)

case class AnswerTable(id: Long, user_id: String, ans_arry: ListBuffer[String], update_time: Long)

object UserAnswerObject {
  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()
    var sparkConf: SparkConf = new SparkConf().setAppName("UserAnswerObject")
    sparkConf.set("spark.rpc.message.maxSize", "256")
    sparkConf.set("spark.network.timeout", "3600")
    sparkConf.set("spark.debug.maxToStringFields", "100")
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
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    /*
    暂停使用
    val answerData = NfRiskAssessmentUserCommitRecordToHive.getUserAnsFromMysql()
    val answerRdd = spark.sparkContext.parallelize(answerData, 100)
    NfRiskAssessmentUserCommitRecordToHive.insertAnswerToHive(answerRdd,spark)*/
    import spark.implicits._
    val dataFrame = spark.sql("select  ID,ACCOUNT_ID,USER_ANS,UPDATE_TIME from db_sscf.nf_risk_assessment_user_commit_record")
    val ansData = dataFrame.as[AnswersBean]
    NfRiskAssessmentUserCommitRecordToHive.insertAnswerNew(ansData, spark)
    spark.close()
  }
}

