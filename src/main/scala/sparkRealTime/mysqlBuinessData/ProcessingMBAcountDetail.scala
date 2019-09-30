package sparkRealTime.mysqlBuinessData

import com.alibaba.fastjson.{JSON, JSONObject}
import hadoopCode.kudu.KuduUtils
import org.apache.spark.rdd.RDD
import scalaUtil.DateScalaUtil
import sparkRealTime.mysqlBuinessData.ProcessingMBOrderData.getDictColumn

import scala.collection.mutable

/**
  * ClassName ProcessingMBData
  * Description //TODO db_investment.t_user_pay_record -------> impala::kudu_real.t_user_pay_record
  * Author lenovo
  * Date 2019/9/18 17:29
  **/
object ProcessingMBAcountDetailData {
  //todo
  def doProcessingMBData(oneRdd: RDD[(String, String)], db_s: String, tb_s: String, db_t: String, tb_t: String, pk: String, kuduTb: String): Unit = {
    val filterData = oneRdd.map(one => {
      JSON.parseObject(one._2)
    }).filter(one => {
      val db_name = one.getString("database")
      val tb_name = one.getString("table")
      db_name.equals(db_s) && tb_name.equals(tb_s)
    })
    filterData.foreachPartition(it => {
      val session = KuduUtils.getManualSession
      it.foreach(line => {
        var json = new JSONObject();
        line.containsKey("table-alter")
        if (!line.containsKey("table-alter")) {
          json = line.getJSONObject("data")
        } else {
          val sqlStr = line.getString("sql")
          println("------------------------------------------修改语句" + sqlStr)
        }
        val typeStr = line.getString("type")
        typeStr match {
          case "insert" => ProcessingMBOrderData.doUpsert3(kuduTb, session, json)
          case "update" => ProcessingMBOrderData.doUpsert3(kuduTb, session, json)
          case "delete" => ProcessingMBOrderData.doDelete3(kuduTb, session, json)
          case "table-alter" => ProcessingMBOrderData.doDDL4(json.getString("sql"), tb_s, kuduTb)
          case _ =>
        }
      })
      session.flush()
      KuduUtils.closeSession()
    })
  }

  def main(args: Array[String]): Unit = {
  }
}