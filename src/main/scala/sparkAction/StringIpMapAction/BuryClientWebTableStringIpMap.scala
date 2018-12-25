package sparkAction.StringIpMapAction

import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.{BuryLogin, BuryMainFunction}

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  */
object BuryClientWebTableStringIpMap {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableClientWebStringIpMap")
  def cleanClientWebData(filterWeb: RDD[BuryLogin], hc: HiveContext,dayFlag:Int,dict:Array[String]): Unit = {
    /**
    　　* @Description: 清洗出手机客户端镶嵌WEB的数据
    　　* @param [filterWeb, hc, diffDay]
    　　* @return void
    　　* @throws
    　　* @author lenovo
    　　* @date 2018/12/4 17:45
    　　*/
    val map: RDD[Row] = filterWeb.map(line => {
      val all: String = line.line
      val ipStr = line.ipStr
      val split: Array[String] = all.split("\\|")
      val hashMap = new mutable.HashMap[String,String]()
      split.foreach(l => {
        val i = l.indexOf("=")//获取第一个等于号的位置
        if (i > 0) {
          //如果长度为2
          val strfirst = l.substring(0,i)
          val strSecond = l.substring(i+1,l.length)
          val trimKey: String = strfirst.trim
          val trimVal: String = strSecond.trim
          val bool = BuryMainFunction.selectStockField(dict,trimKey)
          if(!bool){
            hashMap+=((trimKey,trimVal))
          }
        }
      })
      Row(all,ipStr,hashMap)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(map, StructUtil.structCommonStringIpMap)
    createDataFrame.registerTempTable("StockShopClientWebMap")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag,1)
    hc.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopClientWebMap")
  }
}
