package sparkAction

import com.niufu.tar.bec.BullBearTrendIndicatorCache
import com.qq.tars.protocol.tars.BaseDecodeStream
import conf.ConfigurationManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalaUtil.{LocalOrLine, RandomCharData}
import sparkAction.MasterTraderRedBlue.MasterTraderRedBlueToHive

import scala.collection.{JavaConversions, JavaConverters, mutable}
import scala.collection.mutable.ArrayBuffer

/**
  * ClassName MasterTraderRedBlueObject
  * Description // TODO:  获取每只股票用操盘大师的涨跌幅原始数据
  * Author wangyd
  * Date 2019/12/26 9:40
  **/

case class UllBearTrendCache(data_key: String, data_value: String, update_time: String)


object MasterTraderRedBlueObject {
  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()
    var sparkConf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.set("spark.rpc.message.maxSize", "256")
    sparkConf.set("spark.network.timeout", "3600")
    sparkConf.set("spark.debug.maxToStringFields", "100")
    if (LocalOrLine.isWindows) {
      System.setProperty("HADOOP_USER_NAME", "hive")
      sparkConf.setMaster("local[*]")
      println("----------------------------------------------------------------------------开发模式")
    }
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    //import spark.implicits._
    // TODO: 获取每只股票用操盘大师的涨跌幅原始数据
    import spark.implicits._
    val dataFrame = spark.sql("select data_key,data_value,update_time from db_sscf.nf_bull_bear_trend_cache")
    val ullBearTrendCacheData = dataFrame.as[UllBearTrendCache]
    MasterTraderRedBlueToHive.insertToHive(ullBearTrendCacheData, spark)
    spark.close()
  }
}
