package testBury.StreamingTest

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
object OnlineScala {

  def main(args: Array[String]): Unit = {
    val streamingContext = StreamingContext.getActiveOrCreate(loadProperties("streaming.checkpoint.path"), createContextFunc())

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def createContextFunc(): () => _root_.org.apache.spark.streaming.StreamingContext = {
    () => {
      // 创建sparkConf
      val sparkConf = new SparkConf().setAppName("online").setMaster("local[*]")
      // 配置sparkConf优雅的停止
      sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
      // 配置Spark Streaming每秒钟从kafka分区消费的最大速率
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 指定Spark Streaming的序列化方式为Kryo方式
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 指定Kryo序列化方式的注册器
      sparkConf.set("spark.kryo.registrator", "com.atguigu.registrator.MyKryoRegistrator")

      // 创建streamingContext
      val interval = loadProperties("streaming.interval")
      val streamingContext = new StreamingContext(sparkConf, Seconds(interval.toLong))
      // 启动checkpoint
      val checkPointPath = loadProperties("streaming.checkpoint.path")
      streamingContext.checkpoint(checkPointPath)

      // 获取kafka配置参数
      val kafka_brokers = loadProperties("kafka.broker.list")
      val kafka_topic = loadProperties("kafka.topic")
      var kafka_topic_set : Set[String] = Set(kafka_topic)
      val kafka_group = loadProperties("kafka.groupId")

      // 创建kafka配置参数Map
      val kafkaParam = Map(
        "bootstrap.servers" -> kafka_brokers,
        "group.id" -> kafka_group
      )



      streamingContext
    }
  }



  def getHBaseTabel(prop: Properties) = {
    // 创建HBase配置
    val config = HBaseConfiguration.create
    // 设置HBase参数
    config.set("hbase.zookeeper.property.clientPort", loadProperties("hbase.zookeeper.property.clientPort"))
    config.set("hbase.zookeeper.quorum", loadProperties("hbase.zookeeper.quorum"))
    // 创建HBase连接
    val connection = ConnectionFactory.createConnection(config)
    // 获取HBaseTable
    val table = connection.getTable(TableName.valueOf("online_city_click_count"))
    table
  }

  def dateToString(date:Date): String ={
    val dateString = new SimpleDateFormat("yyyy-MM-dd")
    val dateStr = dateString.format(date)
    dateStr
  }


  def getProperties():Properties = {
    val properties = new Properties()
    val in = OnlineScala.getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(in);
    properties
  }

  def loadProperties(key:String):String = {
    val properties = new Properties()
    val in = OnlineScala.getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(in);
    properties.getProperty(key)
  }

}
