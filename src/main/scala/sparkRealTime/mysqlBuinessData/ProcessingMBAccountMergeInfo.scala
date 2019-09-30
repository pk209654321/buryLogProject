package sparkRealTime.mysqlBuinessData

import com.alibaba.fastjson.{JSON, JSONObject}
import hadoopCode.kudu.KuduUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import scalaUtil.DateScalaUtil

/**
  * ClassName ProcessingMBData
  * Description //TODO db_investment.t_user_pay_record -------> impala::kudu_real.t_user_pay_record
  * Author lenovo
  * Date 2019/9/18 17:29
  **/
object ProcessingMBAccountMergeInfo {
  //todo
  def doProcessingMBData(oneRdd: RDD[(String, String)], db_s: String, tb_s: String, db_t: String, tb_t: String, kuduTb: String): Unit = {
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
        var jsonData = new JSONObject();
        line.containsKey("table-alter")
        if (!line.containsKey("table-alter")) {
          jsonData = line.getJSONObject("data")
          val updatetime = jsonData.getString("updatetime")
          if (StringUtils.isNotBlank(updatetime) && !updatetime.equals("0000-00-00 00:00:00")) {
            jsonData.put("updatetime", DateScalaUtil.getAddEight(0, updatetime, 8))
          } else {
            jsonData.put("updatetime", null)
          }
        } else {
          val sqlStr = line.getString("sql")
          println("------------------------------------------修改语句" + sqlStr)
        }
        val typeStr = line.getString("type")
        typeStr match {
          case "insert" => ProcessingMBOrderData.doUpsert3(kuduTb, session, jsonData)
          case "update" => ProcessingMBOrderData.doUpsert3(kuduTb, session, jsonData)
          case "delete" => ProcessingMBOrderData.doDelete3(kuduTb, session, jsonData)
          case "table-alter" => ProcessingMBOrderData.doDDL4(line.getString("sql"), tb_s, kuduTb)
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
