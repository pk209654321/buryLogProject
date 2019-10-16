package testBury.StructedStreamingTest

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
  * ClassName test1
  * Description TODO
  * Author lenovo
  * Date 2019/10/10 9:48
  **/
object test1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-2.6.4")
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.getLabel)
    import spark.implicits._
    val topic = "buryTest"
    val df = spark
      //read是批量读取，readStream是流读取，write是批量写,writeStream是流写入 关于startingoffsets "latest" for streaming, "earliest" for batch
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "nfbigdata-54:9092")
      .option("subscribe", topic) //topic可以订阅多个，消费具体分区用assign,消费topic用subscribe
       //     .option("startingoffsets", "earliest") 读具体偏移量，只支持批读取
      //.option("endingoffsets", "latest")
      .load()
    val kafkaDf: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    //判断是否为流处理
    println(kafkaDf.isStreaming)
    kafkaDf.printSchema()
    //val words:Dataset[Row] = kafkaDf.map(_._2).toDF("name")
    val words:Dataset[Row] = kafkaDf.map(one=> {
      one._2.substring(0)
    }).toDF("name")
    val query = words
      .writeStream
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "e://hdfs/aaa")
      .foreach(new MysqlSink2())
      .trigger(Trigger.ProcessingTime(10000))
      .start()
    query.awaitTermination()
  }
}
