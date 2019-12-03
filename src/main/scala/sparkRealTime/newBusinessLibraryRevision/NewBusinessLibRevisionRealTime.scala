package sparkRealTime.newBusinessLibraryRevision

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalaUtil.{ExceptionMsgUtil, LocalOrLine, MailUtil}
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * ClassName BuryLogRealTimeMysql
  * Description TODO 实时推送数据到crm系统中:1,用户最后在线时间 2,用户id
  * Author lenovo
  * Date 2019/3/5 14:10
  **/
object NewBusinessLibRevisionRealTime {

  def main(args: Array[String]): Unit = {

    val load = ConfigFactory.load()
    //获取偏移量表名称
    val offsetTable = load.getString("kafka.mysqlNewLib.offset")
    //kafka地址
    val kbl = load.getString("kafka.broker.list")
    //消费组
    val kmg = load.getString("kafka.mysqlNewLib.groupId")
    //消费主题
    val kmt = load.getString("kafka.mysqlNewLib.topics")
    // 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> kbl,
      "group.id" -> kmg,
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString
    )
    val topics = kmt.split(",").toSet

    val sparkConf = new SparkConf().setAppName("BuryLogRealTimeForOnLine")
    //sparkConf.set("spark.streaming.kafka.maxRatePartition", "1000") //每个partitioin每秒处理的条数
    //sparkConf.set("spark.streaming.backpressure.enabled", "true") //反压
    if (LocalOrLine.isWindows) {
      sparkConf.setMaster("local[*]")
      println("----------------------------开发模式")
      //return
    }
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    // TODO:
    DBs.setup('offset)
    val fromOffsets: Map[TopicAndPartition, Long] = NamedDB('offset).readOnly { implicit session =>
      SQL("select * from " + offsetTable + " where groupid=? and topic=?").bind(kmg, kmt).map(rs => {
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
      //todo 测试
      //ProcessingMBData.doProcessingMBData(oneRdd, "phpmanager", "user_test", "default", "user_test", "id","impala::default.user_test")
      try {
        if (!oneRdd.isEmpty()) {
          try {
            ProcessingMBGiftOrder.doProcessingMBData(oneRdd, "db_order", "t_gift_order", "kudu_ods", "t_gift_order", "impala::kudu_ods.t_gift_order")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_gift_order" + ExceptionMsgUtil.getStackTraceInfo(e))
          }

          try {
            ProcessingMBHistoryOrder.doProcessingMBData(oneRdd, "db_order", "t_history_order", "kudu_ods", "t_history_order", "impala::kudu_ods.t_history_order")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_history_order" + ExceptionMsgUtil.getStackTraceInfo(e))
          }

          try {
            ProcessingMBHistoryOrderDetail.doProcessingMBData(oneRdd, "db_order", "t_history_order_detail", "kudu_ods", "t_history_order_detail", "impala::kudu_ods.t_history_order_detail")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_history_order_detail" + ExceptionMsgUtil.getStackTraceInfo(e))
          }

          try {
            ProcessingMBOfflineOrder.doProcessingMBData(oneRdd, "db_order", "t_offline_order", "kudu_ods", "t_offline_order", "impala::kudu_ods.t_offline_order")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_offline_order" + ExceptionMsgUtil.getStackTraceInfo(e))
          }

          try {
            ProcessingMBOrder.doProcessingMBData(oneRdd, "db_order", "t_order", "kudu_ods", "t_order", "impala::kudu_ods.t_order")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_order" + ExceptionMsgUtil.getStackTraceInfo(e))
          }
          try {
            ProcessingMBOrderDetail.doProcessingMBData(oneRdd, "db_order", "t_order_detail", "kudu_ods", "t_order_detail", "impala::kudu_ods.t_order_detail")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_order_detail" + ExceptionMsgUtil.getStackTraceInfo(e))
          }
          try {
            ProcessingMBPermission.doProcessingMBData(oneRdd, "db_permissions", "t_permission", "kudu_ods", "t_permission", "impala::kudu_ods.t_permission")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_permissions.t_permission" + ExceptionMsgUtil.getStackTraceInfo(e))
          }
          try {
            ProcessingMBPermissionDetail.doProcessingMBData(oneRdd, "db_permissions", "t_permission_detail", "kudu_ods", "t_permission_detail", "impala::kudu_ods.t_permission_detail")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_permissions.t_permission_detail" + ExceptionMsgUtil.getStackTraceInfo(e))
          }
          try {
            ProcessingMBProduct.doProcessingMBData(oneRdd, "db_goods", "t_product", "kudu_ods", "t_product", "impala::kudu_ods.t_product")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_goods.t_product" + ExceptionMsgUtil.getStackTraceInfo(e))
          }


          //实时处理
          val offsetRanges = oneRdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //偏移量新处理方式
          val offsetInfos = offsetRanges.map(line => {
            Seq(line.topic, kmg, line.partition, line.untilOffset)
          })

          NamedDB('offset).localTx {
            implicit session =>
              SQL("REPLACE INTO " + offsetTable + " (topic, groupid, partitions,offset) VALUES (?,?,?,?)")
                .batch(offsetInfos: _*).apply()
          }
        }
      } catch {
        case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "同步失败" + ExceptionMsgUtil.getStackTraceInfo(e)); ssc.stop()
      }
    })
    // 启动程序，等待程序终止
    ssc.start()
    ssc.awaitTermination()
  }

}
