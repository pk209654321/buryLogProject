package sparkAction.mapAction

import java.util.Random

import bean.{Data, UserLoginStr}
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import scalaUtil.{DateScalaUtil, HttpPostUtil}
import sparkAction.BuryLogin

/**
  * Created by lenovo on 2018/11/26.
  */
object BuryLoginReportNew {
  private val httpUrl: String = ConfigurationManager.getProperty("http.url")

  def repotUserLogin(accessData: RDD[BuryLogin]): Unit = {
    /**
      * 　　* @Description: 用户登录上报(http端口)
      * 　　* @param [accessData]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2018/12/4 17:47
      * 　　*/

    val userTimeRdd = accessData.map(one => {
      val line = one.line
      val strings: Array[String] = line.split("\\|")
      var user_id=""
      var access_time=""
      for (elem <- strings) {
        var trimKey = ""
        var trimValue = ""
        if (StringUtils.isNotBlank(elem)) {
          val strings: Array[String] = elem.split("=")
          if (strings.length > 1) {
            trimKey = strings(0).trim
            trimValue = strings(1).trim
          }
        }
        trimKey match {
          case "user_id" => user_id = trimValue
          case "access_time" => access_time = trimValue
          case _ =>
        }
      }
      (user_id, List(access_time))
    })

    //过滤掉user_id是0的,去重
    val disRdd = userTimeRdd.filter(line => {
      val user_id = line._1
      val access_time = line._2.head
      StringUtils.isNotBlank(user_id) && StringUtils.isNotBlank(access_time) && user_id != "0"
  }).distinct()
    //聚合
    val userIdTimeList = disRdd.reduceByKey(_ ::: _)
    //取每个user_id的前一百条
    userIdTimeList.foreach(line => {
      val user_id = line._1
      line._2.sorted.take(100).foreach(access_time => {
        try {
        val time_login: String = DateScalaUtil.tranTimeToString(access_time, 0)
        val data: Data = new Data()
        data.setTime_login(time_login)
        val userStr: UserLoginStr = new UserLoginStr()
        userStr.setType(3)
        userStr.setUserId(user_id.toInt)
        userStr.setData(data)
        HttpPostUtil.sendMessage(userStr, httpUrl)
        } catch {
          case e:Throwable => e.printStackTrace()
        }
      })
    })
  }

  def repotUserLoginNew(accessData: RDD[BuryLogin]): Unit = {
    /**
      * 　　* @Description: 用户登录上报(http端口)
      * 　　* @param [accessData]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2018/12/4 17:47
      * 　　*/
    val userTimeRdd= accessData.map(one => {
      val line = one.line
      val strings: Array[String] = line.split("\\|")
      if (strings.length>=18){
        val user_id = strings(0) //user_id 用户id
        val access_time = strings(2) //access_time 用户登录时间
        (user_id, List(access_time))
      }else{
        ("",List(""))
      }
    })
    //过滤掉user_id是0的,去重
    val disRdd = userTimeRdd.filter(line => {
      val user_id = line._1
      val access_time = line._2.head
      StringUtils.isNotBlank(user_id) && StringUtils.isNotBlank(access_time) && user_id != "0"
    }).distinct()
    //聚合
    val userIdTimeList = disRdd.reduceByKey(_:::_)
    userIdTimeList.foreach(line=> {
      val user_id = line._1
      val time_list = line._2
      time_list.sorted.take(100).foreach(access_time => {
        try {
          val time_login: String = DateScalaUtil.tranTimeToString(access_time, 0)
          val data: Data = new Data()
          data.setTime_login(time_login)
          val userStr: UserLoginStr = new UserLoginStr()
          userStr.setType(3)
          userStr.setUserId(user_id.toInt)
          userStr.setData(data)
          HttpPostUtil.sendMessage(userStr, httpUrl)
        } catch {
          case e:Throwable => e.printStackTrace()
        }
      })
    })
  }


}

case class TempObject(var user_id: String, var access_time: String)