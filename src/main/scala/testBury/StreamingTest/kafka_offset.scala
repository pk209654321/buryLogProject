package testBury.StreamingTest

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafka_offset {
  def main(args: Array[String]): Unit = {
  //基本参数就不做什么解释了。。。。
      val conf = new SparkConf().setAppName("kafka_test").setMaster("local[4]")
      val streamingContext = new StreamingContext(conf,Seconds(5))
      val kafkaParams = Map[String,Object](
        "bootstrap.servers" -> "10.204.118.101:9092,10.204.118.102:9092,10.204.118.103:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test-consumer",
        "auto.offset.reset" -> "latest",//也可以设置earliest和none，
        //将提交设置成手动提交false，默认true，自动提交到kafka，
        "enable.auto.commit" -> "false"
      )
      
    val topics = Array("test")
   /* val Dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics,kafkaParams)
    )

    Dstream.foreachRDD(rdd =>{
      //此处获取rdd个分区offset值
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitions =>{
      //此处将遍历分区，得到每个分区的fromOffset和untilOffset
        val o = offsetRanges(TaskContext.get().partitionId())
        //打印到控制台可以明了的查看offset值
        println(o.fromOffset+"- - - - - - - - - - "+o.untilOffset)
        partitions.foreach(line =>{
        //将分区中数据的key，value值打印到控制台
          println("key"+line.key()+"...........value"+line.value())
        })
      })
      //手动提交处理后的offset值到kafka
      Dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })*/
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}