package testBury.scalaTest

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{LocalOrLine, MailUtil}
import sparkAction.AnswersBean
import sparkAction.portfolioHive.{NfRiskAssessmentUserCommitRecordToHive, PortfolioProSecInfoHiveInsertObject, UserPushButtonInsertToHive}


/**
  * ClassName PortfolioMainFunction
  * Description TODO 解析答题数据
  * Author lenovo
  * Date 2019/2/12 9:16
  **/
object UserAnswerObjectTest {
  def main(args: Array[String]): Unit = {
    try {
      val local: Boolean = LocalOrLine.judgeLocal()
      var sparkConf: SparkConf = new SparkConf().setAppName("UserAnswerObjectTest")
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
      //获取用户开关数据
      //========================================================================================
      //val answerData = NfRiskAssessmentUserCommitRecordToHive.getUserAnsFromMysql()
      //val answerRdd = spark.sparkContext.parallelize(answerData, 1)
      import spark.implicits._
      val dataFrame = spark.sql("select  ID,ACCOUNT_ID,USER_ANS,UPDATE_TIME from db_sscf.nf_risk_assessment_user_commit_record")
      spark.sql("select * from table_temp ").show()
      spark.close()
    } catch {
      case e: Throwable => e.printStackTrace(); //MailUtil.sendMailNew("spark答题数据", "解析失败-----"+e.getMessage)
    }
  }
}

