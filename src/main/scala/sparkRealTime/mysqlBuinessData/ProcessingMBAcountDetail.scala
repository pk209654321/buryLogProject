package sparkRealTime.mysqlBuinessData

import com.alibaba.fastjson.{JSON, JSONObject}
import hadoopCode.kudu.KuduUtils
import org.apache.spark.rdd.RDD
import scalaUtil.MailUtil

/**
  * ClassName ProcessingMBData
  * Description //TODO db_investment.t_user_pay_record -------> impala::kudu_real.t_user_pay_record
  * Author lenovo
  * Date 2019/9/18 17:29
  **/
object ProcessingMBAcountDetail {
  //todo
  def doProcessingMBData(oneRdd: RDD[(String, String)], db_s: String, tb_s: String, db_t: String, tb_t: String, pk: String, kuduTb: String): Unit = {
    val filterData = oneRdd.map(one => {
      JSON.parseObject(one._2)
    }).filter(one => {
      val db_name = one.getString("database")
      val tb_name = one.getString("table")
      db_s.equals(db_name) && tb_s.equals(tb_name)
    })
    filterData.foreachPartition(it => {
      val session = KuduUtils.getManualSession
      it.foreach(line => {
        var json = new JSONObject();
        val typeStr = line.getString("type")
        if (typeStr.equals("insert")||typeStr.equals("update")||typeStr.equals("delete")) {
          json = line.getJSONObject("data")
          if (json != null) {

          } else {
            MailUtil.sendMailNew("业务数据同步Kudu_error_line",line.toJSONString)
          }
        } else {
          val sqlStr = line.getString("sql")
          println("------------------------------------------修改语句" + sqlStr)
        }
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
}
