package hadoopCode.sparkRealTime

import java.util
import java.util.Date

import conf.ConfigurationManager
import hadoopCode.hbaseFormal.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalaUtil.{DateScalaUtil, MailUtil, RandomCharData}
import sparkAction.{BuryLogin, BuryMainFunction}


/**
  * Created by zx on 2017/10/17.
  */
object KafkaWordCount2 {
  private val HBASE_TABLE_NAME: String = ConfigurationManager.getProperty("hbase.tablename")
  private val KAFKA_ZK_QUORUM: String = ConfigurationManager.getProperty("kafka.zk.quorum")
  private val KAFKA_GROUP_NAME: String = ConfigurationManager.getProperty("kafka.group.name")
  private val KAFKA_TOPIC_NAME: String = ConfigurationManager.getProperty("kafka.topic.name")

  def main(args: Array[String]): Unit = {

   /* val className = this.getClass.getSimpleName

    val conf = new SparkConf().setAppName(s"$className").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val zkQuorum = KAFKA_ZK_QUORUM
    val groupId = KAFKA_GROUP_NAME
    val topic = Map[String, Int](KAFKA_TOPIC_NAME -> 1)

    //创建DStream，需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    //对数据进行处理
    val lines: DStream[String] = data.map(_._2)
    val filterNull = lines.filter(line => {
      //过滤空行和有ip却为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    //转化为BuryLogin
    val clean: DStream[BuryLogin] = filterNull.map(BuryMainFunction.cleanCommonFunction)
    val visitData = clean.filter(_.logType == 1) //访问日志
    val visitGuid = visitData.filter(_.line.indexOf("guid=")>=0)//过滤出有guid的数据
    val rowKeyContent = visitGuid.map(one => {
      var time = one.sendTime
      if (StringUtils.isNumeric(time)) {
        time = DateScalaUtil.tranTimeToString(time, 0)
      }
      time = time.replaceAll("-", "")
        .replaceAll(":", "")
        .replaceAll(" ", "")
        .trim
      val code = HBaseUtilTest.getPartitonCode(time, 10)
      val guid = findCommonFun(one.line,5,"guid=")
      val accessTime = findCommonFun(one.line,12,"access_time=")
      val offlineTime = findCommonFun(one.line,13,"offline_time=")
      val rowKey = HBaseUtilTest.getVisitRowKey(code,time,guid,offlineTime)
      (rowKey, one.line)
    })
    rowKeyContent.foreachRDD(rdds => {
      rdds.foreachPartition(par => {
        //val list = new util.ArrayList[HbaseBean]()
        HBaseUtilTest.init("")
        val puts = new util.ArrayList[Put]()
        par.foreach(line => {
          val rowKey = line._1
          val value = line._2
          val put = new Put(Bytes.toBytes(rowKey))
          put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("content"),Bytes.toBytes(value))
          puts.add(put)
        })
        try {
          val l = HBaseUtilTest.putByHTable(HBASE_TABLE_NAME, puts)
          println("hbase insert success 耗费时间:"+l)
        } catch {
          case e:Exception => MailUtil.sendMail("sparkstreaming error","hbase insert 失败:"+e.getMessage);e.printStackTrace()
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()*/
  }

  //找出guid,offline_time,access_time 值
  def findCommonFun(line:String,start:Int,goal:String): String ={
    var temp=""
    val gIndex = line.indexOf(goal)
    if(gIndex>=0){//如果能获取到
      val strGuid = line.substring(gIndex)
      val i = strGuid.indexOf("|")
      temp = strGuid.substring(start, i)
    }
    if(StringUtils.isBlank(temp)|| goal=="offline_time="){
      temp=RandomCharData.createRandomCharData(10)
    }else{
      temp=temp.substring(0,10)
    }
    return  temp
  }

}
