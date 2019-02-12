package hadoopCode.sparkRealTime

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * ClassName RealTimeDirect
  * Description TODO
  * Author lenovo
  * Date 2019/1/31 16:28
  **/
object RealTimeDirect {
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> "kafka.broker.list",
      "group.id" -> "kafka.group.id",
      "auto.offset.reset" -> "smallest"
    )
    val topics = "kafka.topics".split(",").toSet
    val ssc = new StreamingContext(conf, Seconds(10))

    // 加载配置信息
    DBs.setup()
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly{implicit session =>
      sql"select * from streaming_offset_24 where groupid=?".bind("kafka.group.id").map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }.toMap

    if(kafkaParams.size==0){//第一次启动
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    }else{
      var checkedOffset = Map[TopicAndPartition, Long]()
      val kafkaCluster = new KafkaCluster(kafkaParams)
      val earliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)
      if (earliestLeaderOffsets.isRight) {
        val topicAndPartitionToOffset = earliestLeaderOffsets.right.get

        // 开始对比
        checkedOffset = fromOffsets.map(owner => {
          val clusterEarliestOffset = topicAndPartitionToOffset.get(owner._1).get.offset
          if (owner._2 >= clusterEarliestOffset) {
            owner
          } else {
            (owner._1, clusterEarliestOffset)
          }
        })
      }
      // 程序菲第一次启动
      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, checkedOffset, messageHandler)
    }


  }
}
