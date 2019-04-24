package testBury.scalaTest

import java.util.Date

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scalaUtil.{DateScalaUtil, LocalOrLine}
import sparkAction.BuryLogin
import sparkAction.BuryMainFunction.{dict, hdfsPath}
import sparkAction.buryCleanUtil.BuryCleanCommon

/**
  * ClassName TempRunBury
  * Description TODO 该类是用于将日志平台中的日志按时间过滤,进行数据重刷
  * Author lenovo
  * Date 2018/12/28 9:01
  **/
object RestartBury {
  private val filterTime: String = "" //按什么时间过滤
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
    val realPath = "E:\\desk\\日志out\\rz"
    val file: RDD[String] = sc.textFile(realPath, 1)
    val filterBlank: RDD[String] = file.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    //清洗去掉不规则字符
    val allData = filterBlank.map(BuryCleanCommon.cleanCommonToListBuryLogin).filter(_.size() > 0)
    val rddOneObjcet: RDD[AnyRef] = allData.flatMap(_.toArray())
    val allDataOneRdd = rddOneObjcet.map(_.asInstanceOf[BuryLogin]).filter(one => {
      val line = one.line
      StringUtils.isNotBlank(line)
    })

    val filter04 = allDataOneRdd.filter(one => {
      val sendTime = one.sendTime
      var tmp = ""
      if (StringUtils.isNumeric(sendTime)) {
        tmp = DateScalaUtil.tranTimeToString(sendTime, 1)
      } else {
        val date: Date = DateScalaUtil.string2Date(sendTime, 1)
        tmp = DateScalaUtil.date2String(date, 1)
      }
      if (tmp.equals("2019-04-04")) {
        true
      } else {
        false
      }
    })
    filter04.map(one=> {
      val ipStr = one.ipStr
      //val jsonStr = JSON.toJSONString(one, SerializerFeature.WriteNullListAsEmpty)
      val gson = new Gson()
      one.ipStr=null
      val jsonStr = gson.toJson(one)
      jsonStr+"&"+ipStr
    }).coalesce(1).saveAsTextFile("E:\\desk\\日志out\\rzout")
    /* if (buryLogin.source > 0) {
       val time = buryLogin.sendTime
       var tmp = ""
       if (StringUtils.isNumeric(time)) {
         tmp = DateScalaUtil.tranTimeToString(time, 1)
       } else {
         val date: Date = DateScalaUtil.string2Date(time, 1)
         tmp = DateScalaUtil.date2String(date, 1)
       }
       if (tmp == filterTime) {
         line
       } else {
         ""
       }
     } else {
       ""
     }
   }

   ).filter(StringUtils.isNotBlank(_)).coalesce(1).saveAsTextFile("e:\\wang08")*/
  }

}
