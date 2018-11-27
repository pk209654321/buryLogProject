package sparkAction

import java.util.Date

import bean.{UserLoginStr, Data}
import com.alibaba.fastjson.JSON
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scalaUtil.{LocalOrLine, HttpPostUtil, DateScalaUtil}

/**
  * Created by lenovo on 2018/10/25.
  */
object UserLogin {
  private val url=ConfigurationManager.getProperty("http.url")
  private val hdfsPath: String = ConfigurationManager.getProperty("hdfs.log")

  def getPreviousDateStr(): String ={
    val nextDate: Date = DateScalaUtil.getNextDate(new Date(),-1)
    val string: String = DateScalaUtil.date2String(nextDate,2)
    string
  }
  def main(args: Array[String]) {
    val local: Boolean = LocalOrLine.judgeLocal()
    var sparkConf: SparkConf = new SparkConf().setAppName("UserLogin")
    if (local){
      sparkConf=sparkConf.setMaster("local[1]")
    }
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
    val file: RDD[String] = sparkContext.textFile(hdfsPath+"18-11-13")
    val loginData: RDD[BuryLogin] = file.filter(line => {
      StringUtils.isNotBlank(line)
    })
      .map(line => {
          val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
          val parseObject: BuryLogin = JSON.parseObject(all, classOf[BuryLogin])
          parseObject
      })
      .filter(_.line!= null)
      .filter(_.logType == 1)
      .filter(one => {
        //访问日志
        //过滤出访问日志
        val line: String = one.line
        val a: Int = line.indexOf("access_time")
        val o: Int = line.indexOf("offline_time")
        if (a >= 0 && o < 0) {
          //只有登录时间
          true
        } else {
          false
        }
      })
       loginData .foreach(one => {
        val line: String = one.line
        val splitL: Array[String] = line.split("\\|")
        val map: mutable.HashMap[String, String] = mutable.HashMap[String, String]()
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
        HttpPostUtil.sendMessage(userStr,url)
      })
  }
}

case class DateStr(user_ids: mutable.HashMap[String, String], time_login: String)