package testBury

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.dengtacj.bec.ProSecInfoList
import com.qq.tars.protocol.tars.BaseDecodeStream
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Json
import scalaUtil.{DateScalaUtil, HttpPostUtil, LocalOrLine}
import scalikejdbc.{DB, NamedDB, SQL}
import scalikejdbc.config.DBs
import sparkAction.{Portfolio, PortfolioStr}
import sparkAction.StringIpActionListHive.BuryClientWebTableStringIp.TABLE

import scala.collection.mutable
import scala.util.parsing.json
import scala.util.parsing.json.JSONObject

/**
  * ClassName TempTest
  * Description TODO
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object ScalaTest {
  //  def main(args: Array[String]): Unit = {
  //    val map = mutable.HashMap[String,Int]()
  //        for (a <- 1 to 1000){
  //          var time = DateScalaUtil.getPreviousDateStr(a,0)
  //          time = time.replaceAll("-", "")
  //            .replaceAll(":", "")
  //            .replaceAll(" ", "")
  //            .trim
  //          val str = HBaseUtilTest.getPartitonCode(time,10)
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
  //      SQL("select * from t_portfolio").map(rs => PortfolioBean(rs.string("sKey"),rs.bytes("sValue"),rs.string("updatetime"))).list().apply()
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

  //  def main(args: Array[String]): Unit = {
  //    var a="PROFOGUID:0db8a994caa52f868906ce5dae5cb403"
  //    val i = a.indexOf(":")
  //    val str = a.substring(i+1)
  //
  //    println(str)
  //  }

//  def main(args: Array[String]): Unit = {
//    getMysqlData
//    val portfolioStrs = getPortfolioFromMysql(getMysqlData)
//    //insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}')
//    NamedDB('hive).localTx { implicit session =>
//      portfolioStrs.foreach(line => {
//        SQL("insert overwrite table  portfolio_test (sKey,sValue,updatetime) values(?,?,?)").bind(line.sKey,2,line.updatetime).update().apply()
//      })
//      NamedDB('hive).close()
//    }
//  }
//
//  def getMysqlData()={
//    DBs.setupAll()
//    val peoples = NamedDB('mysql).readOnly { implicit session =>
//      SQL("select * from t_portfolio").map(rs => Portfolio(rs.string("sKey"),rs.bytes("sValue"),rs.string("updatetime"))).list().apply()
//    }
//    NamedDB('mysql).close()
//    peoples
//  }
//
//  def getPortfolioFromMysql(dataMysql: scala.List[Portfolio]):List[PortfolioStr] ={
//    val portfolioStrs = dataMysql.map(one => {
//      val value = one.sValue
//      val stream = new BaseDecodeStream(value)
//      val list = new ProSecInfoList()
//      list.readFrom(stream)
//      val sValue = JSON.toJSONString(list, SerializerFeature.WriteMapNullValue)
//      PortfolioStr(one.sKey, sValue, one.updatetime)
//    })
//    portfolioStrs
//  }

//  def main(args: Array[String]): Unit = {
//
//    val local: Boolean = LocalOrLine.judgeLocal()
//    var sparkConf: SparkConf = new SparkConf().setAppName("HeraTest")
//    if (local) {
//      //System.setProperty("HADOOP_USER_NAME", "wangyd")
//      sparkConf = sparkConf.setMaster("local[*]")
//    }
//    val sc = new SparkContext(sparkConf)
//    val allDataRdd = sc.textFile("hdfs://188.185.1.41:8020/wangyadong/hera")
//    println(allDataRdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().toList)
//  }

 /* def main(args: Array[String]): Unit = {
    val load = ConfigFactory.load()
    val topics = load.getString("kafka.topics").split(",").toSet
    println(topics)
  }*/
// def main(args: Array[String]): Unit = {
//   val str="138353|561b848ca33cd50e554192f06a787184|1551960039|1551960040|huawei|2.5.0|DIG-AL00|android|6.0|N/A|4G|720x1208|604|360|70:8A:09:E0:64:E6|866294038131308|89861114250296137592|866294038131308|"
//   println(str(18))
//   println(str.split("\\|",-1).length)
// }

  /*def main(args: Array[String]): Unit = {
    var sparkConf: SparkConf = new SparkConf().setAppName("BuryMainFunction")
    val local: Boolean = LocalOrLine.judgeLocal()
    if (local) {
      //System.setProperty("HADOOP_USER_NAME", "wangyd")
      sparkConf = sparkConf.setMaster("local[*]")
    }
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(List(("a",1),("b",1),("c",1),("d",1),("e",1),("f",1)))
    val rdd2 = sc.parallelize(List(("b",1),("c",1),("d",1)))//
    println(rdd1.leftOuterJoin(rdd2).filter(_._2._2.isEmpty).collect().toList)

  }*/

  def main(args: Array[String]): Unit = {
    val load = ConfigFactory.load()
    val str = load.getString("crm.userStatus.url")
    println(str)
  }

}

