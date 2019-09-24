package testBury.StreamingTest

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.LocalOrLine
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * ClassName BuryLogRealTimeMysql
  * Description TODO 实时推送数据到crm系统中:1,用户最后在线时间 2,用户id
  * Author lenovo
  * Date 2019/3/5 14:10
  **/
object BuryHourStreamingTest {
  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()
    val load = ConfigFactory.load()
    val str = load.getString("dev.flag")
    if (str != "dev") {
      return
    }
    //获取偏移量表名称
    val tableName = load.getString("kafka.offset.table")
    // 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> load.getString("kafka.broker.list"),
      "group.id" -> load.getString("kafka.group.id"),
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString
    )
    val topics = load.getString("kafka.topics").split(",").toSet

    // StreamingContext
    val sparkConf = new SparkConf().setAppName("BuryHourStreamingTest").setMaster("local[*]")
    if (local) {
      sparkConf.setMaster("local[*]")
    }
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint("hdfs://master:8020/user/wangyd/bury")

    // TODO:  
    DBs.setupAll()
    val fromOffsets: Map[TopicAndPartition, Long] = NamedDB('offset).readOnly { implicit session =>
      SQL("select * from " + tableName + " where groupid=? and topic=?").bind(load.getString("kafka.group.id"), load.getString("kafka.topics")).map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }.toMap

    val stream = if (fromOffsets.size == 0) { // 假设程序第一次启动
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else { //程序不是第一次启动
      var checkedOffset = Map[TopicAndPartition, Long]()
      val kafkaCluster = new KafkaCluster(kafkaParams)
      val earliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)
      if (earliestLeaderOffsets.isRight) {
        val topicAndPartitionToOffset = earliestLeaderOffsets.right.get

        // 开始对比
        checkedOffset = fromOffsets.map(owner => {
          //根据kafka中topic和partition获取最早的offset
          val clusterEarliestOffset = topicAndPartitionToOffset.get(owner._1).get.offset
          if (owner._2 >= clusterEarliestOffset) {
            //mysql中的偏移量值大于kafka中的偏移量大小
            owner
          } else {
            (owner._1, clusterEarliestOffset)
          }
        })
      }
      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, checkedOffset, messageHandler)
    }

    val filterStream = stream.transform(line => {
      val v = line.map(_._2)
      BuryHourStreamingObject.doBuryPhoneHourMinute(v)
    })

    val tp_hStream = filterStream.map(line => {
      val timeStr = line._1
      val hash_id = line._2
      val page_id = line._3
      ((timeStr, page_id), hash_id)
    })
    val countDstream = tp_hStream.updateStateByKey(
      (values: Seq[String], state: Option[Int]) => {
        var newValue = state.getOrElse(0)
        val i = values.distinct.count(one => {
          StringUtils.isNotBlank(one)
        })
        Some(newValue + i)
      }).print()
    stream.foreachRDD(oneRdd => {
      //实时处理
      val offsetRanges = oneRdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //偏移量新处理方式
      val offsetInfos = offsetRanges.map(line => {
        Seq(line.topic, load.getString("kafka.group.id"), line.partition, line.untilOffset)
      })

      NamedDB('offset).localTx {
        implicit session =>
          SQL("REPLACE INTO " + tableName + " (topic, groupid, partitions, offset) VALUES (?,?,?,?)")
            .batch(offsetInfos: _*).apply()
      }
    })
    // 启动程序，等待程序终止
    ssc.start()
    ssc.awaitTermination()
  }

}
