package sparkAction.buryCleanUtil

import java.util

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import scalaUtil.MailUtil
import sparkAction.BuryLogin

/**
  * ClassName BuryCleanCommon
  * Description TODO
  * Author lenovo
  * Date 2019/1/29 9:03
  **/
object BuryCleanCommon {
  //查询股市app_web字段,将其他字段放入other_map中
  def selectStockField(dict: Array[String], key: String): Boolean = {
    if (StringUtils.isBlank(key)) {
      return true
    }
    var flag: Boolean = false
    for (elem <- dict if !flag) {
      if (elem == key) {
        //在字典表中找到了改key
        flag = true
      }
    }
    flag
  }

  //获取老版本的日志
  val getOldVersionFunction = (one: BuryLogin) => {
    val line = one.line
    val strings = line.split("\\|")
    val i = strings(0).indexOf("=")
    i >= 0
  }
  //获取新版日志
  val getNewVersionFunction = (one: BuryLogin) => {
    val line = one.line
    val strings = line.split("\\|")
    val i = strings(0).indexOf("=")
    i < 0
  }

  //获取PC端web日志
  val getPcWebLog = (one: BuryLogin) => {
    val line = one.line
    val i = line.indexOf("application=browser")
    if (i > 0) {
      true
    } else {
      false
    }
  }
  //获取手机web日志
  val getPhoneWebLog = (one: BuryLogin) => {
    val line = one.line
    val i = line.indexOf("application=web")
    if (i >= 0) {
      true
    } else {
      false
    }
  }


  //获取客户端行为日志
  val getPhoneClientActionLog = (one: BuryLogin) => {
    val source: Int = one.source
    if (source == 1 || source == 2) {
      //安卓和ios端
      //过滤出手机客户端Data
      true
    } else {
      false
    }
  }
  //清洗原始日志
  val cleanCommonFunction: String => util.List[BuryLogin] = (line: String) => {
    val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
    val jsonAndIp: Array[String] = all.split("&")
    var listbury: java.util.List[BuryLogin] = new util.ArrayList[BuryLogin]()
    if (jsonAndIp.length >= 2) {
      //如果有ip和 httpurl 代'&'的埋点类容
      val ifList = all.indexOf("[")
      //判断是否是打包上传
      var bury = ""
      val i = all.lastIndexOf("&")
      val ipTemp = all.substring(i + 1, all.length)
      if (ifList == 0) { //如果==0 是打包上传传递的是jsonList格式的日志
        bury = all.substring(0, i)
      } else {
        bury = "[" + all.substring(0, i) + "]"
      }
      try {
        listbury = JSON.parseArray(bury, classOf[BuryLogin])
      } catch {
        case e: Throwable => println(s"error_log----------------------${all}")
      }
      for (i <- (0 until listbury.size())) {
        val login = listbury.get(i)
        login.ipStr = ipTemp
      }
      listbury
    } else {
      try {
        JSON.parseArray("[" + all + "]", classOf[BuryLogin])
      } catch {
        case e: Throwable => println(s"error_log-----------------------${all}"); listbury
      }
    }
  }


  //优化cleanCommonFunction清洗代码
  val cleanCommonToListBuryLogin: String => util.List[BuryLogin] = (line: String) => {
    var listbury: java.util.List[BuryLogin] = new util.ArrayList[BuryLogin]()
    val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
    try {
      var bury = ""
      val jsonAndIp: Array[String] = all.split("&")
      val ifList = all.indexOf("[")
      val i = all.lastIndexOf("&")
      val ipTemp = all.substring(i + 1, all.length)
      if (ifList == 0) { //如果==0 是打包上传传递的是jsonList格式的日志
        bury = all.substring(0, i)
      } else {
        bury = "[" + all.substring(0, i) + "]"
      }
      listbury = JSON.parseArray(bury, classOf[BuryLogin])
      for (i <- (0 until listbury.size())) {
        val login = listbury.get(i)
        login.ipStr = ipTemp
      }
      listbury
    } catch {
      case e: Throwable => println(s"error_log-----------------------${all}"); listbury
    }
  }
}
