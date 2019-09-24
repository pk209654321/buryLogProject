package testBury.scalaTest

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalaUtil.LocalOrLine

/**
  * ClassName HbaseTest1
  * Description TODO
  * Author lenovo
  * Date 2019/9/10 17:12
  **/
object HbaseTest1 {
  def main(args: Array[String]): Unit = {
    //Spark Conf配置信息
    val local: Boolean = LocalOrLine.judgeLocal()
    var sparkConf: SparkConf = new SparkConf().setAppName("PortfolioMainFunctionTest2")
    if (local) {
      System.setProperty("HADOOP_USER_NAME", "wangyd")
      sparkConf = sparkConf.setMaster("local[*]")
    }

    //初始化SparkSession对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //初始化HBase Configuration
    val hbaseconf = HBaseConfiguration.create()
    //创建HBaseContext对象
    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseconf)
    //准备一个RDD，后面用于向HBase表插入数据
    val rdd = spark.sparkContext.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes("INFO"), Bytes.toBytes("a"), Bytes.toBytes("1")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes("INFO"), Bytes.toBytes("b"), Bytes.toBytes("2")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes("INFO"), Bytes.toBytes("c"), Bytes.toBytes("3")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes("INFO"), Bytes.toBytes("d"), Bytes.toBytes("4")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes("INFO"), Bytes.toBytes("e"), Bytes.toBytes("5"))))
    ))

    val tableName = TableName.valueOf("H_B")
    //使用HBaseContext.bulkPut向指定的HBase表写数据
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      tableName,
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) =>
          put.addColumn(putValue._1, putValue._2, putValue._3)
        )
        put
      });
  }
}
