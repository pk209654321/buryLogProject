package sparkAction.userLoginReport

import bean.StockShopClient
import conf.ConfigurationManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import scalaUtil.{DateScalaUtil, StructUtil}
import sparkAction.BuryLogin

import scala.collection.mutable

/**
  * Created by lenovo on 2018/11/16.
  *
  */
object BuryClientTableMap {
   private val TABLE: String = ConfigurationManager.getProperty("actionTableClient")
  def cleanClientData(filterClient: RDD[BuryLogin],hc: HiveContext,diffDay:Int) ={
    /**
    　　* @Description: 清洗出客户端的数据insert到hive仓库中
    　　* @param [filterClient, hc, diffDay]
    　　* @return org.apache.spark.sql.DataFrame
    　　* @throws
    　　* @author lenovo
    　　* @date 2018/12/4 17:48
    　　*/
    val map: RDD[Row] = filterClient.map(one => {
      val line = one.line
      val split = line.split("\\|")
      val client: StockShopClient = new StockShopClient
      val hashMap: mutable.Map[String, String] = new mutable.HashMap[String,String]()
      split.foreach(l => {
        val splitEQ = l.split("=")
        if (splitEQ.length > 1) {
          //如果长度为2
          val trimKey:String = splitEQ(0).trim
          val trimVal: String = splitEQ(1).trim
          hashMap+=((trimKey,trimVal))
        }
      })
      Row(hashMap)
    })

    val createDataFrame: DataFrame = hc.createDataFrame(map,StructUtil.structClientMap)
    createDataFrame.registerTempTable("StockShopClientMap")
    val timeStr: String = DateScalaUtil.getPreviousDateStr(diffDay,1)
    val hql= s"insert overwrite table ${TABLE} partition(hp_stat_date='${timeStr}') select * from StockShopClientMap"
    hc.sql(hql)

  }
}
