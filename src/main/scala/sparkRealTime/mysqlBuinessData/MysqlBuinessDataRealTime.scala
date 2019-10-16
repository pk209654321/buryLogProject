package sparkRealTime.mysqlBuinessData

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{LocalOrLine, MailUtil}
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * ClassName BuryLogRealTimeMysql
  * Description TODO 实时推送数据到crm系统中:1,用户最后在线时间 2,用户id
  * Author lenovo
  * Date 2019/3/5 14:10
  **/
object MysqlBuinessDataRealTime {

  def main(args: Array[String]): Unit = {

    val local: Boolean = LocalOrLine.judgeLocal()
    val load = ConfigFactory.load()
    //获取偏移量表名称
    val tableName = load.getString("kafka.mysqlBD.offset")
    //kafka地址
    val kbl = load.getString("kafka.broker.list")
    //消费组
    val kmg = load.getString("kafka.mysqlBD.groupId")
    //消费主题
    val kmt = load.getString("kafka.mysqlBD.topics")
    // 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> kbl,
      "group.id" -> kmg,
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
      "enable.auto.commit" -> "false"
    )
    val topics = kmt.split(",").toSet

    val sparkConf = new SparkConf().setAppName("BuryLogRealTimeForOnLine")
    if (LocalOrLine.isWindows) {
      sparkConf.setMaster("local[*]")
      println("----------------------------开发模式")
      //return
    }
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    // TODO:
    DBs.setupAll()
    val fromOffsets: Map[TopicAndPartition, Long] = NamedDB('offset).readOnly { implicit session =>
      SQL("select * from " + tableName + " where groupid=? and topic=?").bind(kmg, kmt).map(rs => {
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
          //实时处理
          val offsetRanges = oneRdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //偏移量新处理方式
          val offsetInfos = offsetRanges.map(line => {
            Seq(line.topic, kmg, line.partition, line.untilOffset)
          })
          NamedDB('offset).localTx {
            ProcessingMBUserInvitation.doProcessingMBData(oneRdd, "db_sscf", "t_user_invitation", "kudu_real", "t_user_invitation", "impala::kudu_real.t_user_invitation")
            //ProcessingMBProduct.doProcessingMBData(oneRdd, "db_sscf", "t_product", "kudu_real", "t_product", "impala::kudu_real.t_product")
            //ProcessingMBPaymanagerSaleStatement.doProcessingMBData(oneRdd, "db_sscf", "paymanager_sale_statement", "kudu_real", "paymanager_sale_statement", "impala::kudu_real.paymanager_sale_statement")
            //ProcessingMBOrderData.doProcessingMBData(oneRdd, "db_investment", "t_user_pay_record", "kudu_real", "t_user_pay_record", "account_id,inner_order", "impala::kudu_real.t_user_pay_record")
            //ProcessingMBAccountMergeInfo.doProcessingMBData(oneRdd, "db_account", "t_account_merge_info", "kudu_real", "t_account_merge_info", "impala::kudu_real.t_account_merge_info")
            //ProcessingMBAcountDetailData.doProcessingMBData(oneRdd, "db_account", "t_account_detail", "kudu_real", "t_account_detail", "iAccountId", "impala::kudu_real.t_account_detail")
            //ProcessingMBAccountMergeInfo.doProcessingMBData(oneRdd, "db_account", "t_account_resigter_info", "kudu_real", "t_account_resigter_info", "impala::kudu_real.t_account_resigter_info")
            implicit session =>
              SQL("REPLACE INTO " + tableName + " (topic, groupid, partitions,offset) VALUES (?,?,?,?)")
                .batch(offsetInfos: _*).apply()
          }
        }
      } catch {
        case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("业务数据同步Kudu", "同步失败"); ssc.stop()
      }
    })
    // 启动程序，等待程序终止
    ssc.start()
    ssc.awaitTermination()
  }

}
