package testBury.scalaTest

import java.util

import bean.shareControl.{BlockInfo, ShareMany, SharePageInfo}
import bean.userChoiceStock.{PortGroupInfo, PortfolioBean}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.dengtacj.bec.{GroupInfo, ProSecInfo, ProSecInfoList}
import com.qq.tars.protocol.tars.BaseDecodeStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{LocalOrLine, MailUtil}
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs
import sparkAction.PortfolioMainFunction._
import sparkAction.PortfolioMysqlData.PortfolioMysqlDataObject
import sparkAction.portfolioHive.{PortfolioProSecInfoHiveInsertObject, UserPushButtonInsertToHive}

import scala.collection.JavaConversions._
import scala.collection.{JavaConversions, mutable}

/**
  * ClassName PortfolioMainFunction
  * Description TODO 用户自选股信息接入,分享控件信息接入,用户股票预警
  * Author lenovo
  * Date 2019/2/12 9:16
  **/
object PortfolioMainFunctionTest {
  def main(args: Array[String]): Unit = {
    try {
      val local: Boolean = LocalOrLine.judgeLocal()
      var sparkConf: SparkConf = new SparkConf().setAppName("PortfolioMainFunction")
      sparkConf.set("spark.rpc.message.maxSize", "256")
      sparkConf.set("spark.driver.extraJavaOptions", "-XX:PermSize=1g -XX:MaxPermSize=2g");
      sparkConf.set("spark.network.timeout","3600")
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
      //val diffDay: Int = args(0).toInt
      //获取用户开关数据
      val userPushData = UserPushButtonInsertToHive.getUserPushData()
      //========================================================================================
      val userPushRdd: RDD[(Long, mutable.Map[Integer, Integer], String)] = spark.sparkContext.parallelize(userPushData,20)
      UserPushButtonInsertToHive.doUserPushButtonInsertToHive(userPushRdd,spark)
      spark.close()
    } catch {
      case e: Throwable => e.printStackTrace(); //MailUtil.sendMail("spark用户自选股解析入库|分享控件|用户股票预警", "失败")
    }
  }
}

