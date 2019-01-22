package sparkAction.mapAction

import bean.{Data, UserLoginStr}
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import scalaUtil.{DateScalaUtil, HttpPostUtil}
import sparkAction.BuryLogin
import sparkAction.BuryLoginReport.httpUrl

/**
  * Created by lenovo on 2018/11/26.
  */
object BuryLoginReportNew {
  private val httpUrl: String = ConfigurationManager.getProperty("http.url")
  def repotUserLogin(accessData: RDD[BuryLogin]): Unit ={
    /**
    　　* @Description: 用户登录上报(http端口)
    　　* @param [accessData]
    　　* @return void
    　　* @throws
    　　* @author lenovo
    　　* @date 2018/12/4 17:47
    　　*/
    val userTimeRdd: RDD[(String, String)] = accessData.map(one => {
      val line = one.line
      val strings: Array[String] = line.split("\\|")
      val tempObject = new TempObject("", "");
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
          case "user_id" => tempObject.user_id = trimValue
          case "access_time" => tempObject.access_time = trimValue
          case _ =>
        }
      }
      (tempObject.user_id, tempObject.access_time)
    })

    //过滤掉user_id是0的,去重
//    val disRdd: RDD[(String, String)] = userTimeRdd.distinct().filter(_._1!="0")
//    disRdd.foreach(line => {
//      val user_id = line._1
//      val access_time = line._2
//      val time_login: String = DateScalaUtil.tranTimeToString(access_time,0)
//      val data: Data = new Data()
//      data.setTime_login(time_login)
//      val userStr: UserLoginStr = new UserLoginStr()
//      userStr.setType(3)
//      userStr.setUserId(user_id.toInt)
//      userStr.setData(data)
//      HttpPostUtil.sendMessage(userStr,httpUrl)
//    })
    val l = userTimeRdd.distinct().filter(_._1!="0").count()
    println("------------------------------------count"+l)



  }
}

case class TempObject(var user_id:String,var access_time:String)