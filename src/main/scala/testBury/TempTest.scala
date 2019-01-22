package testBury

import hadoopCode.hbaseCommon.HBaseUtil
import org.apache.commons.lang3.StringUtils
import scalaUtil.{DateScalaUtil, HttpPostUtil}

import scala.collection.mutable

/**
  * ClassName TempTest
  * Description TODO
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object TempTest {
  def main(args: Array[String]): Unit = {
    val map = mutable.HashMap[String,Int]()
        for (a <- 1 to 1000){
          var time = DateScalaUtil.getPreviousDateStr(a,0)
          time = time.replaceAll("-", "")
            .replaceAll(":", "")
            .replaceAll(" ", "")
            .trim
          val str = HBaseUtil.getPartitonCode(time,10)
          println(str)
          var i = map.getOrElse(str,0)
          if(i==0){//里面数据
            map.+=((str,1))
          }else{
            i+=1
            map.+=((str,i))
          }
        }
   map.foreach(line => println("key:"+line._1+"                   value:"+line._2))
        //val i: Int =map.getOrElse("aaa",0)
  }
}
