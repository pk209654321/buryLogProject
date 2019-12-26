package sparkRealTime.newBusinessLibraryRevision

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.ConfigFactory
import hadoopCode.kudu.KuduUtils
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
    sparkConf.set("spark.streaming.kafka.maxRatePartition", "20000") //每个partitioin每秒处理的条数
    sparkConf.set("spark.streaming.backpressure.enabled", "true");

    //sparkConf.set("spark.streaming.backpressure.enabled", "true") //反压
    if (LocalOrLine.isWindows) {
      sparkConf.setMaster("local[*]")
      println("----------------------------开发模式")
      //return
    }
    val ssc = new StreamingContext(sparkConf, Seconds(30))
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
      try {
        if (!oneRdd.isEmpty()) {
          val filterDataTemp = oneRdd.map(line => {
            try {
              JSON.parseObject(line._2)
            } catch {
              case e: Throwable => println("错误数据==========================" + line._2); new JSONObject()
            }
          }).filter(one => {
            val db_name = one.getString("database")
            val tb_name = one.getString("table")
            if (
              ("db_order".equals(db_name) && "t_gift_order".equals(tb_name)) ||
                ("db_order".equals(db_name) && "t_order".equals(tb_name)) ||
                ("db_order".equals(db_name) && "t_order_detail".equals(tb_name)) ||
                ("db_permissions".equals(db_name) && "t_permission_detail".equals(tb_name)) ||
                ("db_goods".equals(db_name) && "t_product".equals(tb_name))
            ) {
              true
            } else {
              false
            }
          })

          filterDataTemp.foreachPartition(line => {
            try {
              val session = KuduUtils.getManualSession
              line.foreach(one => {
                val db_name = one.getString("database")
                val tb_name = one.getString("table")
                try {
                  if ("db_order".equals(db_name) && "t_gift_order".equals(tb_name)) {
                    ProcessingMBGiftOrder.doProcessingMBData2(session, one, "db_order", "t_gift_order", "kudu_ods", "t_gift_order", "impala::kudu_ods.t_gift_order")
                  }
                } catch {
                  case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_gift_order" + ExceptionMsgUtil.getStackTraceInfo(e))
                }

                try {
                  if ("db_order".equals(db_name) && "t_order".equals(tb_name)) {
                    ProcessingMBOrder.doProcessingMBData2(session, one, "db_order", "t_order", "kudu_ods", "t_order", "impala::kudu_ods.t_order")
                  }
                } catch {
                  case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_order" + ExceptionMsgUtil.getStackTraceInfo(e))
                }

                try {
                  if ("db_order".equals(db_name) && "t_order_detail".equals(tb_name)) {
                    ProcessingMBOrderDetail.doProcessingMBData2(session, one, "db_order", "t_order_detail", "kudu_ods", "t_order_detail", "impala::kudu_ods.t_order_detail")
                  }
                } catch {
                  case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_order_detail" + ExceptionMsgUtil.getStackTraceInfo(e))
                }

                try {
                  if ("db_permissions".equals(db_name) && "t_permission_detail".equals(tb_name)) {
                    ProcessingMBPermissionDetail.doProcessingMBData2(session, one, "db_permissions", "t_permission_detail", "kudu_ods", "t_permission_detail", "impala::kudu_ods.t_permission_detail")
                  }
                } catch {
                  case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_permissions.t_permission_detail" + ExceptionMsgUtil.getStackTraceInfo(e))
                }

                try {
                  if ("db_goods".equals(db_name) && "t_product".equals(tb_name)) {
                    ProcessingMBProduct.doProcessingMBData2(session, one, "db_goods", "t_product", "kudu_ods", "t_product", "impala::kudu_ods.t_product")
                  }
                } catch {
                  case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_goods.t_product" + ExceptionMsgUtil.getStackTraceInfo(e))
                }
              })
              session.flush()
            } finally {
              KuduUtils.closeSession()
            }
          })

          /*try {
            ProcessingMBGiftOrder.doProcessingMBData(filterDataTemp, "db_order", "t_gift_order", "kudu_ods", "t_gift_order", "impala::kudu_ods.t_gift_order")

          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_gift_order" + ExceptionMsgUtil.getStackTraceInfo(e))
          }*/

          /*try {
            ProcessingMBOrder.doProcessingMBData(filterDataTemp, "db_order", "t_order", "kudu_ods", "t_order", "impala::kudu_ods.t_order")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_order" + ExceptionMsgUtil.getStackTraceInfo(e))
          }*/

          /*try {
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
          }*/

          /* try {
             ProcessingMBOrderDetail.doProcessingMBData(filterDataTemp, "db_order", "t_order_detail", "kudu_ods", "t_order_detail", "impala::kudu_ods.t_order_detail")
           } catch {
             case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_order.t_order_detail" + ExceptionMsgUtil.getStackTraceInfo(e))
           }*/
          /* try {
             ProcessingMBPermission.doProcessingMBData(oneRdd, "db_permissions", "t_permission", "kudu_ods", "t_permission", "impala::kudu_ods.t_permission")
           } catch {
             case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_permissions.t_permission" + ExceptionMsgUtil.getStackTraceInfo(e))
           }*/
          /* try {
             ProcessingMBPermissionDetail.doProcessingMBData(filterDataTemp, "db_permissions", "t_permission_detail", "kudu_ods", "t_permission_detail", "impala::kudu_ods.t_permission_detail")
           } catch {
             case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_permissions.t_permission_detail" + ExceptionMsgUtil.getStackTraceInfo(e))
           }*/
         /* try {
            ProcessingMBProduct.doProcessingMBData(filterDataTemp, "db_goods", "t_product", "kudu_ods", "t_product", "impala::kudu_ods.t_product")
          } catch {
            case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("新改版业务数据同步Kudu", "db_goods.t_product" + ExceptionMsgUtil.getStackTraceInfo(e))
          }*/


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
