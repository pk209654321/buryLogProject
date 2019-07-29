package testBury.scalaTest

import java.util

import bean.shareControl.{BlockInfo, ShareMany, SharePageInfo}
import bean.userChoiceStock.{PortGroupInfo, PortfolioBean}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.dengtacj.bec.ProSecInfoList
import com.qq.tars.protocol.tars.BaseDecodeStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{LocalOrLine, MailUtil}
import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}
import sparkAction.PortfolioMysqlData.PortfolioMysqlDataObject
import sparkAction.portfolioHive.PortfolioProSecInfoHiveInsertObject

import scala.collection.JavaConversions._
import scala.collection.{JavaConversions, mutable}

/**
  * ClassName PortfolioMainFunction
  * Description TODO 用户自选股信息接入,分享控件信息接入
  * Author lenovo
  * Date 2019/2/12 9:16
  **/
object PortfolioMainFunctionNew {
  def main(args: Array[String]): Unit = {
    try {
      val local: Boolean = LocalOrLine.judgeLocal()
      var sparkConf: SparkConf = new SparkConf().setAppName("PortfolioMainFunctionNew")
      sparkConf.set("spark.akka.frameSize", "256")
      sparkConf.set("spark.driver.extraJavaOptions", "-XX:PermSize=1g -XX:MaxPermSize=2g");
      sparkConf.set("spark.network.timeout", "3600")
      if (local) {
        System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val sc: SparkContext = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      val spark = SparkSession.builder().config("spark.debug.maxToStringFields", "100")
        .enableHiveSupport()
        .getOrCreate()





      //      PortfolioProSecInfoHiveInsertObject.insertPortfolioManyToHive(manyRdd, spark, diffDay)
      //      PortfolioProSecInfoHiveInsertObject.insertPortfolioToHiveGroupInfo(groups, spark, diffDay)
      //      PortfolioProSecInfoHiveInsertObject.insertShareControl(shareRdds, spark)
      sc.stop()
    } catch {
      case e: Throwable => e.printStackTrace();
    }
  }
}

