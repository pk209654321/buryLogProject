package sparkRealTime.mysqlBuinessData

import com.alibaba.fastjson.{JSON, JSONObject}
import hadoopCode.kudu.KuduUtils
import org.apache.spark.rdd.RDD

/**
  * ClassName ProcessingMBData
  * Description //TODO db_investment.t_user_pay_record -------> impala::kudu_real.t_user_pay_record
  * Author lenovo
  * Date 2019/9/18 17:29
  **/
object ProcessingMBUserInvitation{
  //todo
  def doProcessingMBData(oneRdd: RDD[(String, String)], db_s: String, tb_s: String, db_t: String, tb_t: String, kuduTb: String): Unit = {
    val filterData = oneRdd.map(one => {
      try {
         JSON.parseObject(one._2)
      } catch {
        case _ => println("错误数据=========================="+one._2);new JSONObject()
      }
    }).filter(one => {
      val db_name = one.getString("database")
      val tb_name = one.getString("table")
      db_s.equals(db_name) && tb_s.equals(tb_name)
    })
    filterData.foreachPartition(it => {
      val session = KuduUtils.getManualSession
      it.foreach(line => {
        var jsonData = new JSONObject();
        if (!line.containsKey("table-alter")) {
          jsonData = line.getJSONObject("data")
          ProcessingMBOrderData.getRightTimeByName(jsonData,"authorize_time")
          ProcessingMBOrderData.getRightTimeByName(jsonData,"invite_time")
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
