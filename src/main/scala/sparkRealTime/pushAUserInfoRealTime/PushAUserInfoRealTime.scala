package sparkRealTime.pushAUserInfoRealTime

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import scalaUtil.LocalOrLine

/**
  * ClassName PushAUserInfoRealTime
  * Description TODO
  * Author lenovo
  * Date 2019/4/25 14:43
  **/
object PushAUserInfoRealTime {
  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()
    var sparkConf: SparkConf = new SparkConf().setAppName("PushAUserInfoRealTime")
    if (local) {
      //System.setProperty("HADOOP_USER_NAME", "wangyd")
      sparkConf = sparkConf.setMaster("local[*]")
    }
    //创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val address=Seq(new InetSocketAddress("192.168.136.101",9999))
    val result = FlumeUtils.createPollingStream(ssc,address,StorageLevel.MEMORY_AND_DISK_SER_2)
    val value = result.map(line => {
      val str = new String(line.event.getBody.array())
      str
    })
    value.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
