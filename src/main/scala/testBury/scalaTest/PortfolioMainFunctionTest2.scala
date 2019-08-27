package testBury.scalaTest

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.dengtacj.bec.ProSecInfoList
import com.qq.tars.protocol.tars.BaseDecodeStream
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{LocalOrLine, MailUtil, RandomCharData}
import sparkAction.AnswersBean
import sparkAction.portfolioHive.{NfRiskAssessmentUserCommitRecordToHive, PortfolioProSecInfoHiveInsertObject, UserPushButtonInsertToHive}

import scala.collection.mutable.ArrayBuffer


/**
  * ClassName PortfolioMainFunction
  * Description TODO 解析答题数据
  * Author lenovo
  * Date 2019/2/12 9:16
  **/
object PortfolioMainFunctionTest2 {
  def main(args: Array[String]): Unit = {
    try {
      val local: Boolean = LocalOrLine.judgeLocal()
      var sparkConf: SparkConf = new SparkConf().setAppName("PortfolioMainFunctionTest2")
      sparkConf.set("spark.rpc.message.maxSize", "256")
      sparkConf.set("spark.network.timeout", "3600")
      sparkConf.set("spark.debug.maxToStringFields", "100")
      if (local) {
        System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val spark = SparkSession.builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")

      spark.close()
    } catch {
      case e: Throwable => e.printStackTrace(); //MailUtil.sendMailNew("spark答题数据", "解析失败-----"+e.getMessage)
    }
  }
}

case class SelfBean(skey: String, svalue: String, updatetime: String)

case class Prosecinfo(
                      var brecvannounce:Boolean,
                      var brecvresearch:Boolean,
                      var fchiphighprice:Float,
                      var fchiplowprice:Float,
                      var fdecreasesper:Float,
                      var fhighprice:Float,
                      var fincreaseper:Float,
                      var flowprice:Float,
                      var fmainchiphighprice:Float,
                      var fmainchiplowprice:Float,
                      var icreatetime:Int,
                      var iupdatetime:Int,
                      var iversion:Int,
                      var isaialert:Boolean,
                      var isdel:Boolean,
                      var isdkalert:Boolean,
                      var sdtseccode:String,
                      var ishold:Boolean,
                      var skey:String,
                      var sname:String,
                      var stcommentinfo_icreatetime:Int,
                      var stcommentinfo_iupdatetime:Int,
                      var stcommentinfo_scomment:String,
                      var vbroadcasttime:java.util.List[Integer],
                      var vstrategyid:java.util.List[Integer],
                      var updatetime:String
)

