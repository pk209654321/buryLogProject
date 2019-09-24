package testBury.StreamingTest

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import scalaUtil.DateScalaUtil
import sparkAction.BuryLogin
import sparkAction.buryCleanUtil.BuryCleanCommon

/**
  * ClassName BuryHourStreamingObject
  * Description TODO
  * Author lenovo
  * Date 2019/9/11 11:13
  **/
object BuryHourStreamingObject {

  def main(args: Array[String]): Unit = {
    val line="from_sign_id=201812071|hash_id=c23a0c6f85d8a51cbaa8d0dace35bc20|id=zwxz_page.ljxz|opentime=20190911 19:34:41|createtime=20190911 19:37:15|application=web|platform=android|url=https://download.gushi.com/app/download/index.html?from_sign_id=201812071|task_id=1001377"
    val str = println(getValFromLine(line, "hash_id="))

  }

  def doBuryHour(oneRdd: RDD[String]) ={
    val beanBury = oneRdd.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
      .map(BuryCleanCommon.cleanCommonToListBuryLogin).filter(_.size() > 0).flatMap(_.toArray())
      .map(_.asInstanceOf[BuryLogin]).filter(one => {
      val line = one.line
      StringUtils.isNotBlank(line)
    })
    val ugdRdd = beanBury.filter(_.logType == 1).map(one => {
      val line = one.line
      val time = one.sendTime
      val strTime = DateScalaUtil.judgeTime(time)
      val splits = line.split("|", -1)
      val user_id = splits(0)
      val guid = splits(1)
      val download_channel = splits(4)
      (strTime,user_id, guid, download_channel)
    })
    ugdRdd
  }
  
  
  def getValFromLine(line:String,key:String) ={
    val index = line.indexOf(key)
    val subLine = line.substring(index+1)
    val eqIndex = subLine.indexOf("=")
    val endIndex = subLine.indexOf("|")
    subLine.substring(eqIndex+1,endIndex)
  }
  
  def doBuryPhoneHourMinute(oneRdd: RDD[String]) ={
    val beanBury = oneRdd.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    }).map(BuryCleanCommon.cleanCommonToListBuryLogin).filter(_.size() > 0).flatMap(_.toArray())
      .map(_.asInstanceOf[BuryLogin]).filter(one => {
      val line = one.line
      StringUtils.isNotBlank(line)
    }).filter(BuryCleanCommon.getOldVersionFunction)
      .filter(BuryCleanCommon.getPhoneWebLog)
    beanBury.map(one=> {
      val lineStr = one.line
      println("-------------------------------------------------------"+lineStr)
      val hash_id = getValFromLine(lineStr,"|hash_id=")
      val id = getValFromLine(lineStr,"|id=")
      val page_id = id.split("\\.")(0)
      val time = getValFromLine(lineStr,"|createtime=")
      val timeStr = DateScalaUtil.judgeTime(time)
      (timeStr,hash_id,page_id)
    })
  }
}
