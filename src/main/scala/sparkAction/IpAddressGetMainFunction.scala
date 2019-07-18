package sparkAction

import conf.ConfigurationManager
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import scalaUtil.{DateScalaUtil, IpAddressUtil, LocalOrLine}
import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}
/**
  * ClassName IpAddressGetMainFunction
  * Description TODO
  * Author lenovo
  * Date 2019/7/11 9:36
  **/
object IpAddressGetMainFunction {
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")
  def main(args: Array[String]): Unit = {
    try {
      val local: Boolean = LocalOrLine.judgeLocal()
      var sparkConf: SparkConf = new SparkConf().setAppName("IpAddressGetMainFunction")
      if (local) {
        //System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val spark = SparkSession.builder().
        config(sparkConf).
        enableHiveSupport().
        getOrCreate()
      val diffDay: Int = args(0).toInt
      val realPath = hdfsPath + DateScalaUtil.getPreviousDateStr(diffDay, 2)
      val dataString = spark.read.textFile(realPath).repartition(10).persist()
      val length = dataString.rdd.partitions.length
      println(s"数据分区数目------${length}")
      //过滤出有ip的
      val dataFilter = dataString.filter(_.split("&").length>=2)
      import spark.implicits._
      val ipStrData = dataString.map(line => {
        val i = line.lastIndexOf("&")
        val ipStr = line.substring(i + 1)
        ipStr
      })
      val dis = ipStrData.distinct()
      DBs.setupAll()
      dis.foreach(line => {
        val addr = IpAddressUtil.getAddress(line)
        println(s"IP地址解析为-----------------${addr}")
        NamedDB('ipAddr).localTx{
          implicit session =>
            SQL("REPLACE INTO t_sip_address (sip, address) VALUES (?,?)").bind(line,addr)
              .update().apply()
        }
      })
      spark.stop()
    } catch {
      case e: Throwable => e.printStackTrace(); //MailUtil.sendMail("极光效果推送警报", e.toString)
    }
  }


  def getIp(): Unit = {

  }
}
