package hadoopCode.sparkRealTime

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zx on 2017/10/17.
  */
object KafkaWordCount2 {


  def main(args: Array[String]): Unit = {

    val className = this.getClass.getSimpleName

    val conf = new SparkConf().setAppName(s"$className").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(10))

    val zkQuorum = "master:2181"
    val groupId = "group_test1"
    val topic = Map[String, Int]("test1" -> 1)

    //创建DStream，需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    //对数据进行处理
    val lines: DStream[String] = data.map(_._2)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    reduced.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
