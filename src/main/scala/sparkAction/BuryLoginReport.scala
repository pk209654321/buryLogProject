package sparkAction

import bean.userLogin.{Data, UserLoginStr}
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scalaUtil.{DateScalaUtil, HttpPostUtil}

/**
  * Created by lenovo on 2018/11/26.
  * -------------------------------暂时弃用
  */
object BuryLoginReport {
  private val httpUrl: String = ConfigurationManager.getProperty("http.url")
  def repotUserLogin(accessData: RDD[BuryLogin]): Unit ={
    val onlyAccData: RDD[BuryLogin] = accessData.filter(one => {
      val line: String = one.line
      if (line.contains("access_time") && !line.contains("offline_time")) {
        //过滤出登录日志
        true
      } else {
        false
      }
    })

    println("用户登录总次数:"+onlyAccData.count())
    onlyAccData.foreach(one => {
      val line: String = one.line
      val splitL: Array[String] = line.split("\\|")
      var time_login = ""
      var user_id:Int=0
      for (l <- splitL) {
        val splitEQ: Array[String] = l.split("=")
        if (splitEQ.length > 1) {
          //map.put(splitEQ(0), splitEQ(1))
          //println(map.toBuffer)
          val key: String = splitEQ(0).trim
          val value: String = splitEQ(1).trim
          if (key == "access_time") {
            //得到登录时间
            if(StringUtils.isNotBlank(value)){
              val time: String = DateScalaUtil.tranTimeToString(value,0)
              println(splitEQ(1)+"------"+time)
              time_login = time
            }
          }
          if(key=="user_id"&&StringUtils.isNotBlank(value)){
            user_id=value.toInt
          }
        }
      }
      val data: Data = new Data()
      data.setTime_login(time_login)
      val userStr: UserLoginStr = new UserLoginStr()
      userStr.setType(3)
      userStr.setUserId(user_id)
      userStr.setData(data)
      HttpPostUtil.sendMessage(userStr,httpUrl)
    })
  }
}
