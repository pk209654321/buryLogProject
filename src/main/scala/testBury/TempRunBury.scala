package testBury

import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import hadoopCode.hbaseCommon.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scalaUtil.{DateScalaUtil, LocalOrLine, MailUtil}
import sparkAction.{BuryLogin, BuryMainFunction}
import sparkAction.BuryMainFunction.cleanCommonFunction
import sparkAction.mapIpAction.BuryClientWebTableMapIp
import sparkAction.mapIpActionList.BuryPhoneWebTableMapIpList

import scala.collection.mutable

/**
  * ClassName TempRunBury
  * Description TODO
  * Author lenovo
  * Date 2018/12/28 9:01
  **/
object TempRunBury {

  //判断当前的日志是否为新版本获得老版本的日志
  val oldVersionFunction= (list:util.List[BuryLogin])=>{
    val line = list.get(0).line
    val strings = line.split("\\|")
    val i = strings(0).indexOf("=")
    i>=0
  }
  val newVersionFunction=(list:util.List[BuryLogin])=>{
    val line = list.get(0).line
    val strings = line.split("\\|")
    val i = strings(0).indexOf("=")
    i<0
  }

  val cleanCommonFunctionTestList: String => util.List[BuryLogin] = (line: String) => {
    val all: String = line.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\u003d", "=")
    val jsonAndIp: Array[String] = all.split("&")
    var listbury:java.util.List[BuryLogin]=new util.ArrayList[BuryLogin]()
    if (jsonAndIp.length >= 2) {
      //如果有ip和 httpurl 代'&'的埋点类容
      val ifList = all.indexOf("[")//判断是否是打包上传
      var bury=""
      val i = all.lastIndexOf("&")
      val ipTemp = all.substring(i + 1, all.length)
      if(ifList==0){ //如果==0 是打包上传传递的是jsonList格式的日志
        bury = all.substring(0, i)
      }else{
        bury = "["+all.substring(0, i)+"]"
      }
      try {
        listbury = JSON.parseArray(bury, classOf[BuryLogin])
      } catch {
        case e:Throwable => println(s"error_log:${all}")
      }
      for(i <- (0 to listbury.size()-1)){
        val login = listbury.get(i)
        login.ipStr=ipTemp
      }
      listbury
    } else {
      try {
        JSON.parseArray("["+all+"]", classOf[BuryLogin])
      } catch {
        case e:Throwable => MailUtil.sendMail("spark日志清洗调度","发现错误日志:"+all);listbury
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val local: Boolean = LocalOrLine.judgeLocal()
    //获取当前类的名称
    val className = this.getClass.getSimpleName
    var sparkConf: SparkConf = new SparkConf().setAppName(s"${className}")
    if (local) {
      System.setProperty("HADOOP_USER_NAME", "wangyd")
      sparkConf = sparkConf.setMaster("local[2]")
    }
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val hc: HiveContext = new HiveContext(sc)
    //val hc: HiveContext = new HiveContext(sc)
    val realPath ="E:\\desk\\日志"
    val file: RDD[String] = sc.textFile(realPath, 1)
    val filterBlank: RDD[String] = file.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    val unit: RDD[util.List[BuryLogin]] = filterBlank.map(cleanCommonFunctionTestList)
    val filterAction: RDD[util.List[BuryLogin]] = unit.filter(_.get(0).logType==2).filter(oldVersionFunction) //过滤出行为日志Data
    val webRddList: RDD[util.List[BuryLogin]] = filterAction.filter(line => {
      val login = line.get(0)
      login.source == 3
    })

    BuryPhoneWebTableMapIpList.cleanPhoneWebData(webRddList,hc,0)
  }
}
