package testBury.javaTest

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.SparkSession

/**
  * ClassName PolarLightFunction
  * Description TODO
  * Author lenovo
  * Date 2019/5/17 9:43
  * 极光效果,调用接口推送其数据
  **/
object PolarLightMainFunction {
  def main(args: Array[String]): Unit = {
    //try {
      //val diffDay: Int = args(0).toInt
     /* val local: Boolean = LocalOrLine.judgeLocal()

      var sparkConf: SparkConf = new SparkConf().setAppName("PolarLightFunction")
      if (local) {
        //System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }*/
      val spark = SparkSession.builder().
        appName("PolarLightMainFunction").
        master("local[*]").
        //config("spark.sql.warehouse.dir", "hdfs://nameservice1/user/hive/warehouse").
        enableHiveSupport().
        getOrCreate()
      spark.sql("show databases").show()
      spark.sql("select * from db_ods.t_pc_web_log_dt limit 100 ").show()
     // spark.close()
    /*} catch {
      case e: Throwable => e.printStackTrace() //MailUtil.sendMail("spark日志清洗调度", "清洗日志失败"); e.printStackTrace()
    }*/
  }

}
