package sparkAction.StringIpActionListHive

import java.util

import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin
import sparkAction.buryCleanUtil.BuryCleanCommon

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  */
object BuryClientWebTableStringIp {
  private val TABLE: String = ConfigurationManager.getProperty("actionTableClientWebAll")

  def cleanClientWebData(filterData: RDD[BuryLogin], hc: HiveContext, dayFlag: Int,dict:Array[String]): Unit = {
    /**
      * 　　* @Description: 清洗出手机客户端镶嵌WEB的数据
      * 　　* @param [filterWeb, hc, diffDay]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2018/12/4 17:45
      * 　　*/

    val map: RDD[Row] = filterData.map(one => {
      val all: String = one.line
      val ipStr = one.ipStr
      val splits = all.split("\\|")
      val hashMap = new mutable.HashMap[String,String]()
      splits.foreach(one=> {
        val i = one.indexOf("=")//获取第一个等于号的位置
        if(i>=0){//有等于号
          val strfirst = one.substring(0,i)
          val strSecond = one.substring(i+1,one.length)
          val trimKey: String = strfirst.trim
          val trimVal: String = strSecond.trim
          val bool = BuryCleanCommon.selectStockField(dict,trimKey)
          if(!bool){
            hashMap+=((trimKey,trimVal))
          }
        }
      })
      Row(all, ipStr,hashMap)
    })
    val createDataFrame: DataFrame = hc.createDataFrame(map, StructUtil.structCommonStringIpMap)
    createDataFrame.registerTempTable("tempTable")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(dayFlag, 1)
    hc.sql(s"insert overwrite  table ${TABLE} partition(hp_stat_date='${timeStr}') select * from tempTable")
  }
}
