package sparkRealTime.touTiaoBackRealTime

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{LocalOrLine, MailUtil}
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * ClassName TouTiaoBackObject
  * Description TODO 实时采集登录日志(logtype==1),将匹配的监测数据(注册的设备信息)推送给头条
  * Author lenovo
  * Date 2019/3/5 14:10
  **/
object TouTiaoBackObjectNew {

  def main(args: Array[String]): Unit = {
    val load = ConfigFactory.load()
    //获取偏移量表名称
    val offsetTableName = load.getString("kafka.toutiao.offset")
    //kafka地址
    val kbl = load.getString("kafka.broker.list")
    //消费组
    val kmg = load.getString("kafka.toutiao.groupId")
    //消费主题
    val kmt = load.getString("kafka.toutiao.topics")
    // 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> kbl,
      "group.id" -> kmg,
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString
    )
    val topics = kmt.split(",").toSet

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.set("spark.streaming.backpressure.enabled", "true") //反压
    if (LocalOrLine.isWindows) {
      sparkConf.setMaster("local[*]")
      println("----------------------------开发模式")
      //return
    }
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    //val hiveContext = new HiveContext(sc)
    // TODO:
    DBs.setup('offset)
    val fromOffsets: Map[TopicAndPartition, Long] = NamedDB('offset).readOnly { implicit session =>
      SQL("select * from " + offsetTableName + " where groupid=? and topic=?").bind(kmg, kmt).map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }.toMap

    val stream = if (fromOffsets.size == 0) { // 假设程序第一次启动
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else { //程序不是第一次启动
      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    stream.foreachRDD(oneRdd => {
      try {
        if (!oneRdd.isEmpty()) {
          //业务逻辑处理
          DataProcessingForTouTiao.doDataProcessingForTouTiao(oneRdd)
          //hiveContext.sql("select current_timestamp()").show()
          //偏移量新处理方式
          val offsetRanges = oneRdd.asInstanceOf[HasOffsetRanges].offsetRanges
          val offsetInfos = offsetRanges.map(line => {
            Seq(line.topic, kmg, line.partition, line.untilOffset)
          })
          NamedDB('offset).localTx {
            implicit session =>
              SQL("REPLACE INTO " + offsetTableName + " (topic, groupid, partitions,offset) VALUES (?,?,?,?)")
                .batch(offsetInfos: _*).apply()
          }
        }
      } catch {
        case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("头条推送", "同步失败"); ssc.stop()
      }
    })
    // 启动程序，等待程序终止
    ssc.start()
    ssc.awaitTermination()
  }

}
