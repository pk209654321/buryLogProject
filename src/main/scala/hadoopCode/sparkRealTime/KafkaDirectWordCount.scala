package hadoopCode.sparkRealTime

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by zx on 2017/7/31.
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    val group = "g001"
    val conf = new SparkConf().setAppName(s"").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Duration(5000))
    val topic = "first"
    val brokerList = "leader:9092"

    val zkQuorum = "leader:2181"
    val topics: Set[String] = Set(topic)

    val topicDirs = new ZKGroupTopicDirs(group, topic)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    val zkClient = new ZkClient(zkQuorum)

    val children = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[(String, String)] = null

    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    //如果保存过 offset
    if (children > 0) {
      for (i <- 0 until children) {

        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
	  
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    var offsetRanges = Array[OffsetRange]()

    val transform: DStream[(String, String)] = kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val messages: DStream[String] = transform.map(_._2)

    messages.foreachRDD { rdd =>
      rdd.foreachPartition(partition =>
        partition.foreach(x => {
          println(x)
        })
      )
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }






}
