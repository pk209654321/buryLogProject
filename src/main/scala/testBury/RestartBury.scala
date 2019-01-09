package testBury

import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{DateScalaUtil, LocalOrLine}
import sparkAction.BuryLogin

/**
  * ClassName TempRunBury
  * Description TODO 该类是用于将日志平台中的日志按时间过滤,进行数据重刷
  * Author lenovo
  * Date 2018/12/28 9:01
  **/
object RestartBury {
  private val filterTime:String="" //按什么时间过滤
  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()
    //获取当前类的名称
    val className = this.getClass.getSimpleName
    var sparkConf: SparkConf = new SparkConf().setAppName(s"${className}")
    if (local) {
      System.setProperty("HADOOP_USER_NAME", "wangyd")
      sparkConf = sparkConf.setMaster("local[1]")
    }
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //val hc: HiveContext = new HiveContext(sc)
    val realPath ="E:\\desk\\log"
    val file: RDD[String] = sc.textFile(realPath, 1)
    val filterBlank: RDD[String] = file.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })

    filterBlank.map(line => {
      val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
      val jsonAndIp: Array[String] = all.split("&")
      var buryLogin=BuryLogin("","",0,0,"")
      if (jsonAndIp.length >= 2) {
        //如果有ip和 httpurl 代'&'的埋点类容
        val i = all.lastIndexOf("&")
        val bury = all.substring(0, i)
        val ipTemp = all.substring(i + 1, all.length)
        try {
          buryLogin = JSON.parseObject(bury, classOf[BuryLogin])
        } catch {
          case e:Throwable => println(s"error_log:${all}")
        }
      }

      if(buryLogin.source>0){
        val time = buryLogin.sendTime
        var tmp=""
        if(StringUtils.isNumeric(time)){
          tmp=DateScalaUtil.tranTimeToString(time,1)
        }else{
          val date: Date = DateScalaUtil.string2Date(time,1)
          tmp=DateScalaUtil.date2String(date,1)
        }
        if (tmp==filterTime){
          line
        }else{
          ""
        }
      }else{
        ""
      }
    }).filter(StringUtils.isNotBlank(_)).coalesce(1).saveAsTextFile("e:\\wang08")
  }
}
