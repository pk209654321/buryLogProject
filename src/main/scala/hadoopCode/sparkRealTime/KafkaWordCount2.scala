package hadoopCode.sparkRealTime

import java.util
import java.util.Date

import hadoopCode.hbaseCommon.{HBaseUtil, HbaseBean}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalaUtil.{DateScalaUtil, RandomCharData}
import sparkAction.{BuryLogin, BuryMainFunction}


/**
  * Created by zx on 2017/10/17.
  */
object KafkaWordCount2 {


  def main(args: Array[String]): Unit = {

    val className = this.getClass.getSimpleName

    val conf = new SparkConf().setAppName(s"$className").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val zkQuorum = "leader:2181"
    val groupId = "group_test_1"
    val topic = Map[String, Int]("bury_test_1" -> 1)

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
      val code = HBaseUtil.getPartitonCode(time, 10)
      val guid = findCommonFun(one.line,5,"guid=")
      val accessTime = findCommonFun(one.line,12,"access_time=")
      val offlineTime = findCommonFun(one.line,13,"offline_time=")
      val rowKey = HBaseUtil.getVisitRowKey(code,time,guid,offlineTime)
      (rowKey, one.line)
    })
    rowKeyContent.foreachRDD(rdds => {
      rdds.foreachPartition(par => {
        val list = new util.ArrayList[HbaseBean]()
        par.foreach(line => {
          val bean = new HbaseBean
          bean.setRowKey(line._1)
          bean.setFamily("info1")
          bean.setQualifier("content")
          bean.setValue(line._2)
          list.add(bean)
        })
        HBaseUtil.insertBatch("hbase_bury_test",list)
      })
      HBaseUtil.close()
    })
    ssc.start()
    ssc.awaitTermination()
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
