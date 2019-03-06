package sparkRealTime

import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import hadoopCode.sparkRealTime.KafkaCluster
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs
import sparkAction.BuryLogin
import sparkAction.buryCleanUtil.BuryCleanCommon

/**
  * ClassName BuryLogRealTimeMysql
  * Description TODO
  * Author lenovo
  * Date 2019/3/5 14:10
  **/
object BuryLogRealTimeMysql {
  def main(args: Array[String]): Unit = {
    val load = ConfigFactory.load()
    //获取偏移量表名称
    val tableName = load.getString("kafka.offset.table")
    // 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> load.getString("kafka.broker.list"),
      "group.id" -> load.getString("kafka.group.id"),
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )
    val topics = load.getString("kafka.topics").split(",").toSet

    // StreamingContext
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("BuryLogRealTimeMysql")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // 加载配置信息
    DBs.setupAll()
    val fromOffsets: Map[TopicAndPartition, Long] = NamedDB('offset).readOnly { implicit session =>
      SQL("select * from " + tableName + " where groupid=?").bind(load.getString("kafka.group.id")).map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }.toMap

    val stream = if (fromOffsets.size == 0) { // 假设程序第一次启动
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else {
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
      // 程序菲第一次启动
      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, checkedOffset, messageHandler)
    }

    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        //过滤清洗掉不规则和空数据
        val buryList = par.map(line => BuryCleanCommon.cleanCommonToListBuryLogin(line._2)).filter(_.size() > 0)
        val buryAnyRef = buryList.flatMap(_.toArray)
        val buryOne = buryAnyRef.map(_.asInstanceOf[BuryLogin])
        //获取用户访问日志
        val buryLogins = buryOne.filter(_.logType==1)
        buryLogins.foreach(one => {
          val lineStr = one.line
          val strings = lineStr.split("\\|")
          val i = strings(0).indexOf("=")
          if(i>=0){//旧版日志
            strings.foreach(kv=> {
              val _kv = kv.split("=")
              val key = _kv(0).trim
              val value = _kv(1).trim
              key match {
                case "user_id" =>
                case "guid" =>
                case "access_time" =>
                case "offline_time" =>
                case "download_channel" =>
                case "client_version" =>
                case "phone_model" =>
                case "phone_system" =>
                case "system_version" =>
                case "operator" =>
                case "network" =>
                case "resolution" =>
                case "screen_height" =>
                case "screen_width" =>
                case "mac" =>
                case "ip" =>
                case "imei" =>
                case "iccid" =>
                case "meid" =>
                case "idfa" =>
                case _=>println("其他key------"+key)
              }
            })
          }else{//新版日志

          }
        })
      })
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 记录偏移量
      offsetRanges.foreach(osr => {
        NamedDB('offset).autoCommit { implicit session =>
          SQL("REPLACE INTO " + tableName + " (topic, groupid, partitions, offset) VALUES (?,?,?,?)")
            .bind(osr.topic, load.getString("kafka.group.id"), osr.partition, osr.untilOffset).update().apply()
        }
        // println(s"${osr.topic} ${osr.partition} ${osr.fromOffset} ${osr.untilOffset}")
      })
    })
    // 启动程序，等待程序终止
    ssc.start()
    ssc.awaitTermination()

  }

}
