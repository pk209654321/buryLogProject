package sparkRealTime.buryLogRealTimeForCrm

import java.util
import java.util.Date

import bean.stockCustom.{CustomLine, StockBean}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import rabbitCode.RpcForUserLineClient
import scalaUtil.HttpPostUtil
import scalikejdbc.{NamedDB, SQL}
import sparkAction.BuryLogin
import sparkAction.buryCleanUtil.BuryCleanCommon

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * ClassName RealTimeCrmLineTimeIp
  * Description TODO
  * Author lenovo
  * Date 2019/3/13 14:50
  **/
object RealTimeCrmLineTimeIp {
  def doRealTimeCrmLineTimeIp(oneRdd: RDD[(String, String)], hc: HiveContext, sc: SparkContext): Unit = {
    val dataFrame = hc.sql("select user_id,last_time from wangyadong.in_user_info")
    val inUserRdd = dataFrame.rdd
    val userIdLastTime = inUserRdd.map(one => {
      val userId = one.getString(0)
      val lastTime = one.getString(1)
      (userId, lastTime)
    })
    val uLMap = userIdLastTime.collect().toMap
    val broadcast = sc.broadcast(uLMap).value
    //对当前rdd
    val buryRdd = oneRdd.map(_._2).map(BuryCleanCommon.cleanCommonToListBuryLogin(_))
      .filter(_.size() > 0)
      .flatMap(_.toArray())
      .map(_.asInstanceOf[BuryLogin]).filter(_.logType == 1)
    val userIdTimeList = buryRdd.map(one => {
      val logStr = one.line
      val ipStr = one.ipStr
      val strings = logStr.split("\\|", -1)
      var userId = ""
      var accessTime = ""
      var offineTime = ""
      if (strings(0).indexOf("=") >= 0) {
        //老板日志
        strings.foreach(one => {
          val kv = one.split("=")
          var key = ""
          var value = ""
          if (kv.length == 2) {
            key = kv(0).trim
            value = kv(1).trim
          }
          key match {
            case "user_id" => userId = value
            case "access_time" => accessTime = value
            case "offline_time" => offineTime = value
            case _ =>
          }
        })
      } else {
        //新版日志
        userId = strings(0).trim
        accessTime = strings(2).trim
        offineTime = strings(3).trim
      }
      (userId, List(accessTime, offineTime))
    })
    //map端关联
    val userIdListOption = userIdTimeList.map(one => {
      val userId = one._1
      val listTime = one._2
      val option = broadcast.get(userId)
      (userId, listTime, option)
    })
    val filterUser = userIdListOption.filter(_._3.isEmpty).filter(!_._1.equals("0")).map(line => (line._1, line._2))
    //val filterUser = userIdTimeList.leftOuterJoin(userIdLastTime).filter(_._2._2.isEmpty).filter(_._1!="0").map(one => (one._1,one._2._1))
    val userIdListTime = filterUser.reduceByKey(_ ::: _)
    userIdListTime.map(one => (one._1, one._2.max)).foreachPartition(par => {
      //val customBeans = new mutable.ListBuffer[CustomLineBean]
      val customLines = new util.ArrayList[CustomLine]()
      par.foreach(line => {
        val userId = line._1
        val time = line._2
        try {
          val cus = new CustomLine
          cus.setUser_id(userId.toInt)
          cus.setLast_line_time(time.toInt)
          customLines.add(cus)
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      })
      val stockBean = new StockBean
      stockBean.setData(customLines)
      HttpPostUtil.sendMessage(stockBean,"http://localhost:8384/bigdata/test")
    })
  }
}

case class CustomLineBean(use_id: Int, last_line_time: Int)

