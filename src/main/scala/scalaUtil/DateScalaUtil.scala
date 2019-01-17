package scalaUtil

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by lenovo on 2018/10/26.
  */
object DateScalaUtil {
  private val fastDateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  private val fastDateFormat1: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
  private val fastDateFormat2: FastDateFormat = FastDateFormat.getInstance("yy-MM-dd")
  private val fastDateFormat3: FastDateFormat = FastDateFormat.getInstance("yyMMddHHmmss")
  private val fastDateFormat4: FastDateFormat = FastDateFormat.getInstance("yyMMdd")
  //时间戳格式化toString
  def tranTimeToString(dateLong:String,flag:Int) :String={
    flag match {
      case 0 => {
        fastDateFormat.format(new Date((dateLong+"000").toLong))
      }
      case 1=> {
        fastDateFormat1.format(new Date((dateLong+"000").toLong))
      }
      case 2=> {
        fastDateFormat2.format(new Date((dateLong+"000").toLong))
      }
      case 3=> {
        fastDateFormat3.format(new Date((dateLong+"000").toLong))
      }
    }
  }

  //将date时间转换成字符串
  def date2String(date: Date,flag:Int):String={
    flag match {
      case 0 => {
        fastDateFormat.format(date)
      }
      case 1=> {
        fastDateFormat1.format(date)
      }
      case 2=> {
        fastDateFormat2.format(date)
      }
      case 3=> {
        fastDateFormat3.format(date)
      }
      case 4=> {
        fastDateFormat4.format(date)
      }
    }
  }

  //将时间字符串转换成date
  def string2Date(dateStr:String,flag:Int):Date={
    flag match {
      case 0 => {
        fastDateFormat.parse(dateStr)
      }
      case 1=> {
        fastDateFormat1.parse(dateStr)
      }
      case 2=> {
        fastDateFormat2.parse(dateStr)
      }
      case 3=> {
        fastDateFormat3.parse(dateStr)
      }
    }
  }

  //时间加减
  def getNextDate(date:Date,num:Int):Date={
    val instance: Calendar = Calendar.getInstance()
    instance.setTime(date)
    instance.add(Calendar.DAY_OF_MONTH,num)
    instance.getTime
  }

  def getPreviousDateStr(offset:Int,timeType:Int): String ={
    val nextDate: Date = DateScalaUtil.getNextDate(new Date(),offset)
    val string: String = DateScalaUtil.date2String(nextDate,timeType)
    string
  }



  def getTest(aa:String){

  }


  def main(args: Array[String]): Unit = {
//    val nextDate: Date = getNextDate(new Date(),-1)
//    val string: String = date2String(nextDate,2)
//    println(string)
    val string: String = tranTimeToString("1540435521802",0)
    println(string)
  }
}
