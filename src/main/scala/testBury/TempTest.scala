package testBury

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.dengtacj.bec.ProSecInfoList
import com.qq.tars.protocol.tars.BaseDecodeStream
import hadoopCode.hbaseCommon.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.json4s.jackson.Json
import scalaUtil.{DateScalaUtil, HttpPostUtil}
import scalikejdbc.{DB, NamedDB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable
import scala.util.parsing.json
import scala.util.parsing.json.JSONObject

/**
  * ClassName TempTest
  * Description TODO
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object TempTest {
  //  def main(args: Array[String]): Unit = {
  //    val map = mutable.HashMap[String,Int]()
  //        for (a <- 1 to 1000){
  //          var time = DateScalaUtil.getPreviousDateStr(a,0)
  //          time = time.replaceAll("-", "")
  //            .replaceAll(":", "")
  //            .replaceAll(" ", "")
  //            .trim
  //          val str = HBaseUtil.getPartitonCode(time,10)
  //          println(str)
  //          var i = map.getOrElse(str,0)
  //          if(i==0){//里面数据
  //            map.+=((str,1))
  //          }else{
  //            i+=1
  //            map.+=((str,i))
  //          }
  //        }
  //   map.foreach(line => println("key:"+line._1+"                   value:"+line._2))
  //        //val i: Int =map.getOrElse("aaa",0)
  //  }
//  def main(args: Array[String]): Unit = {
//    val list1 = List("a", "b", "c")
//    val list2 = List("d")
//    val str1="a"
//    val str2="b"
//    str1::str2::Nil
//    val strings = list1:::list2
//    val listTwo = list1++list2
//    println(listTwo.mkString(","))
//  }
//  def main(args: Array[String]): Unit = {
//    val str="40890|45689d4a56574893b0a25ec97f177ade|1548770148|1548770238|TEST_1|2.4.0|Nexus 5|android|5.1|N/A|wifi|1080x1776|1776|1080|8C:3A:E3:96:60:65|359250050436862| |359250050436862|"
//    val strings = str.split("\\|")
//    val size = strings.size
//    println(size)
//    println(strings.length)
//  }

//  def main(args: Array[String]): Unit = {
//    DBs.setupAll()
//    val peoples = NamedDB('mysql).readOnly { implicit session =>
//      SQL("select * from t_portfolio").map(rs => Portfolio(rs.string("sKey"),rs.bytes("sValue"),rs.string("updatetime"))).list().apply()
//    }
//    val portfolioStrs = peoples.map(one => {
//      val value = one.sValue
//      val stream = new BaseDecodeStream(value)
//      val list = new ProSecInfoList()
//      list.readFrom(stream)
//      println(JSON.toJSONString(list, SerializerFeature.WriteMapNullValue))
//      val sValue = JSON.toJSONString(list, SerializerFeature.WriteMapNullValue)
//      PortfolioStr(one.sKey, sValue, one.updatetime)
//    })
//    portfolioStrs
//  }

}

