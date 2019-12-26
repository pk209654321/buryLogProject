package sparkRealTime.newBusinessLibraryRevision

import com.alibaba.fastjson.{JSON, JSONObject}
import hadoopCode.kudu.KuduUtils
import org.apache.kudu.client.KuduSession
import org.apache.spark.rdd.RDD
import scalaUtil.MailUtil
import sparkRealTime.mysqlBuinessData.ProcessingMBOrderData

/**
  * ClassName ProcessingMBData
  * Description TODO db_investment.t_user_pay_record -------> impala::kudu_real.t_user_pay_record
  * Author lenovo
  * Date 2019/9/18 17:29
  **/
object ProcessingMBProduct {
  def doProcessingMBData(filterDataTemp: RDD[JSONObject], db_s: String, tb_s: String, db_t: String, tb_t: String, kuduTb: String): Unit = {
    val filterData = filterDataTemp.filter(one => {
      val db_name = one.getString("database")
      val tb_name = one.getString("table")
      db_s.equals(db_name) && tb_s.equals(tb_name)
    })
    filterData.foreachPartition(it => {
      try {
        val session = KuduUtils.getManualSession
        it.foreach(line => {
          var jsonData = new JSONObject();
          val typeStr = line.getString("type")
          if (typeStr.equals("insert") || typeStr.equals("update") || typeStr.equals("delete")) {
            jsonData = line.getJSONObject("data")
            if (jsonData != null) {
              ProcessingMBOrderData.getRightTimeByName(jsonData, "ctime")
              ProcessingMBOrderData.getRightTimeByName(jsonData, "utime")
            } else {
              MailUtil.sendMailNew("业务数据同步Kudu_error_line", line.toJSONString)
            }
          } else {
            val sqlStr = line.getString("sql")
            println("------------------------------------------修改语句" + sqlStr)
          }
          typeStr match {
            case "insert" => ProcessingMBOrderData.doUpsert3(kuduTb, session, jsonData)
            case "update" => ProcessingMBOrderData.doUpsert3(kuduTb, session, jsonData)
            case "delete" => ProcessingMBOrderData.doDelete3(kuduTb, session, jsonData)
            case "table-alter" => ProcessingMBOrderData.doDDL4(line.getString("sql"), tb_s, kuduTb)
            case _ =>
          }
        })
        session.flush()
      } finally {
        KuduUtils.closeSession()
      }
    })
  }


  def doProcessingMBData2(session: KuduSession, line: JSONObject, db_s: String, tb_s: String, db_t: String, tb_t: String, kuduTb: String): Unit = {
    var jsonData = new JSONObject();
    val typeStr = line.getString("type")
    if (typeStr.equals("insert") || typeStr.equals("update") || typeStr.equals("delete")) {
      jsonData = line.getJSONObject("data")
      if (jsonData != null) {
        ProcessingMBOrderData.getRightTimeByName(jsonData, "ctime")
        ProcessingMBOrderData.getRightTimeByName(jsonData, "utime")
      } else {
        MailUtil.sendMailNew("业务数据同步Kudu_error_line", line.toJSONString)
      }
    } else {
      val sqlStr = line.getString("sql")
      println("------------------------------------------修改语句" + sqlStr)
    }
    typeStr match {
      case "insert" => ProcessingMBOrderData.doUpsert3(kuduTb, session, jsonData)
      case "update" => ProcessingMBOrderData.doUpsert3(kuduTb, session, jsonData)
      case "delete" => ProcessingMBOrderData.doDelete3(kuduTb, session, jsonData)
      case "table-alter" => ProcessingMBOrderData.doDDL4(line.getString("sql"), tb_s, kuduTb)
      case _ =>
    }
  }


  def doProcessingMBData3(session: KuduSession, line: scala.Iterator[JSONObject], db_s: String, tb_s: String, db_t: String, tb_t: String, kuduTb: String): Unit = {
    line.foreach(jsonObject => {
      val db_name = jsonObject.getString("database")
      val tb_name = jsonObject.getString("table")
      val bool = db_s.equals(db_name) && tb_s.equals(tb_name)

      if (bool) {
        var jsonData = new JSONObject();
        val typeStr = jsonObject.getString("type")
        if (typeStr.equals("insert") || typeStr.equals("update") || typeStr.equals("delete")) {
          jsonData = jsonObject.getJSONObject("data")
          if (jsonData != null) {
            ProcessingMBOrderData.getRightTimeByName(jsonData, "ctime")
            ProcessingMBOrderData.getRightTimeByName(jsonData, "utime")
          } else {
            MailUtil.sendMailNew("业务数据同步Kudu_error_line", jsonObject.toJSONString)
          }
        } else {
          val sqlStr = jsonObject.getString("sql")
          println("------------------------------------------修改语句" + sqlStr)
        }
        typeStr match {
          case "insert" => ProcessingMBOrderData.doUpsert3(kuduTb, session, jsonData)
          case "update" => ProcessingMBOrderData.doUpsert3(kuduTb, session, jsonData)
          case "delete" => ProcessingMBOrderData.doDelete3(kuduTb, session, jsonData)
          case "table-alter" => ProcessingMBOrderData.doDDL4(jsonObject.getString("sql"), tb_s, kuduTb)
          case _ =>
        }
      }
    })
  }
}
