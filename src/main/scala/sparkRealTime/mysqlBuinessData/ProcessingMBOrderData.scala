package sparkRealTime.mysqlBuinessData

import com.alibaba.fastjson.{JSON, JSONObject}
import hadoopCode.kudu.KuduUtils
import hadoopCode.kudu.agent.KuduAgent
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.Type
import org.apache.kudu.client.{KuduSession, KuduTable}
import org.apache.spark.rdd.RDD
import scalaUtil.DateScalaUtil

import scala.collection.mutable

/**
  * ClassName ProcessingMBData
  * Description TODO db_investment.t_user_pay_record -------> impala::kudu_real.t_user_pay_record
  * Author lenovo
  * Date 2019/9/18 17:29
  **/
object ProcessingMBOrderData {
  def doProcessingMBData(oneRdd: RDD[(String, String)], db_s: String, tb_s: String, db_t: String, tb_t: String, pk: String, kuduTb: String): Unit = {
    val filterData = oneRdd.map(_._2).filter(line => {
      val temp1 = "\"database\":\"" + db_s + "\",\"table\":\"" + tb_s + "\""
      val idml = line.indexOf(temp1)
      if (idml > -1) true else false
    })

    filterData.foreachPartition(it => {
      val session = KuduUtils.getManualSession
      it.foreach(line => {
        val typeStr = judgeDMLType(line)
        val jSONObject = JSON.parseObject(line)
        var json = new JSONObject();
        if (!typeStr.equals("table-alter")) {
          json = jSONObject.getJSONObject("data")
          val pay_time = json.getString("pay_time")
          val updatetime = json.getString("updatetime")
          val create_time = json.getString("create_time")
          val refund_time = json.getString("refund_time")
          val refund_apply_date = json.getString("refund_apply_date")
          if (StringUtils.isNotBlank(pay_time) && !pay_time.equals("0000-00-00 00:00:00")) {
            json.put("pay_time", DateScalaUtil.getAddEight(0, pay_time, 8))
          } else {
            json.put("pay_time", null);
          }
          if (StringUtils.isNotBlank(updatetime) && !updatetime.equals("0000-00-00 00:00:00")) {
            json.put("updatetime", DateScalaUtil.getAddEight(0, updatetime, 8))
          } else {
            json.put("updatetime", null)
          }
          if (StringUtils.isNotBlank(create_time) && !create_time.equals("0000-00-00 00:00:00")) {
            json.put("create_time", DateScalaUtil.getAddEight(0, create_time, 8))
          } else {
            json.put("create_time", null)
          }
          if (StringUtils.isNotBlank(refund_time) && !refund_time.equals("0000-00-00 00:00:00")) {
            json.put("refund_time", DateScalaUtil.getAddEight(0, refund_time, 8))
          } else {
            json.put("refund_time", null)
          }
          if (StringUtils.isNotBlank(refund_apply_date) && !refund_apply_date.equals("0000-00-00 00:00:00")) {
            json.put("refund_apply_date", DateScalaUtil.getAddEight(0, refund_apply_date, 8))
          } else {
            json.put("refund_apply_date", null)
          }
        } else {
          val sqlStr = jSONObject.getString("sql")
          println("------------------------------------------修改语句" + sqlStr)
        }
        typeStr match {
          case "insert" => doUpsert3(kuduTb, session, json)
          case "update" => doUpsert3(kuduTb, session, json)
          case "delete" => doDelete3(kuduTb, session, json)
          case "table-alter" => doDDL3(line, tb_s, kuduTb)
          case _ =>
        }
      })
      session.flush()
      KuduUtils.closeSession()
    })
  }


  def doUpsert3(tableName: String, session: KuduSession, json: JSONObject) {
    val upsert = KuduUtils.createUpsert(tableName, json)
    session.apply(upsert)
  }

  def doDelete3(tableName: String, session: KuduSession, json: JSONObject) {
    val delete = KuduUtils.createDeleteNew(tableName, json)
    session.apply(delete)
  }

  def doUpsert(line: String, db_t: String, tb_t: String) = {
    println("upsert====:" + line)
    val startI = line.indexOf("\"data\":{\"")
    val endI = line.indexOf("}")
    val sub = line.substring(startI + 8, endI)
    //val reStr = sub.replaceAll("\"", "")
    val kv = sub.split(",", -1)
    var cols = ""
    var values = ""
    for (index <- (0 until kv.length)) {
      val i2 = kv(index).indexOf(":")
      val key = kv(index).substring(0, i2)
      val valStr = kv(index).substring(i2 + 1)
      if (index == kv.length - 1) {
        cols += key
        values += valStr
      } else {
        cols += key + ","
        values += valStr + ","
      }
    }
    cols = cols.replaceAll("\"", "")
    values = values.replaceAll("\"", "'")
    val sql = s"upsert into ${db_t}.${tb_t} (${cols}) values (${values})"
  }

  def getColType(oneDDL: String) = {
    val iS = oneDDL.indexOf("  ")
    val eS = oneDDL.substring(iS).indexOf(" ")
    val colType = oneDDL.substring(iS + 2, eS)
    val im_colt = getDictColumn(colType)
    im_colt
  }


  def doDDL3(line: String, tb_s: String, table_name: String) {
    println("ddl=====line========" + line)
    val sqlStr = "\"sql\":\"ALTER TABLE `" + tb_s + "`"
    val startI = line.indexOf(sqlStr)
    val sub = line.substring(startI)
    val endI = sub.indexOf("}")
    val start_end = sub.substring(sqlStr.length, endI - 1).replaceAll("\\\\r\\\\n", "").replaceAll("`", "")
    val se = start_end.split(",", -1)
    for (index <- (0 until (se.length))) {
      val addF = se(index).indexOf("ADD")
      val dropF = se(index).indexOf("DROP")
      val changeF = se(index).indexOf("CHANGE")
      val local = se(index).split(" ", -1)
      val coln = local(2)
      if (addF > -1) {
        val colt = local(4)
        val im_colt = getDictColumn(colt)
        KuduUtils.alterTableAddColumn(table_name, coln, im_colt)
      }

      if (dropF > -1) {
        KuduUtils.alterTableDeleteColumn(table_name, coln)
      }

      if (changeF > -1) {
        val newName = local(3)
        KuduUtils.alterTableChangeColumn(table_name, coln, newName)
      }
    }
  }


  def doDDL4(line: String, tb_s: String, table_name: String) {
    println("ddl=====line========" + line)
    val ddlStrs = line.split("\r\n", -1)
    for (in <- (0 until (ddlStrs.length))) {
      if (in != 0) {
        val ddlOneStr = ddlStrs(in).replaceAll("(`|,)", "")
        val acStrs = ddlOneStr.split(" ", -1)
        val ac = acStrs(0)
        val colName = acStrs(2)
        val addF = ac.indexOf("ADD")
        val dropF = ac.indexOf("DROP")
        val changeF = ac.indexOf("CHANGE")
        if (addF > -1) {
          val colType = acStrs(4)
          val reType = colType.replaceAll("\\(.*\\)", "")
          val im_colt = getDictColumn(reType)
          KuduUtils.alterTableAddColumn(table_name, colName, im_colt)
        }

        if (dropF > -1) {
          KuduUtils.alterTableDeleteColumn(table_name, colName)
        }

        if (changeF > -1) {
          val newName = acStrs(3)
          KuduUtils.alterTableChangeColumn(table_name, colName, newName)
        }
      }
    }
  }

  def getRightTimeByName(json: JSONObject, filedName: String) {
    val filedNameVal = json.getString(filedName)
    if (StringUtils.isNotBlank(filedNameVal) && !"0000-00-00 00:00:00".equals(filedNameVal)) {
      json.put(filedName, DateScalaUtil.getAddEight(0, filedNameVal, 8))
    } else {
      json.put(filedName, null)
    }
  }


  // TODO: 根据key获取值
  def getValByKey(key: String, line: String) = {
    /**
      * 　　* @Description: //TODO  根据key获取值
      * 　　* @param [key, line]
      * 　　* @return java.lang.String
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/9/23 13:47
      * 　　*/
    val iData = line.indexOf("\"data\":{")
    val subStr = line.substring(iData)
    val endI = subStr.indexOf("}")
    val dataAll = subStr.substring(0, endI)
    val keyStr = "\"" + key + "\":"
    val valSt = dataAll.indexOf(keyStr)
    val valStr = dataAll.substring(valSt)
    val index = valStr.indexOf(",")
    val indexE = valStr.indexOf("}")
    if (index > -1) {
      valStr.substring(keyStr.length, index).replaceAll("\"", "'")
    } else {
      valStr.substring(keyStr.length, indexE).replaceAll("\"", "'")
    }
  }


  def judgeDMLType(line: String) = {
    val strFlag = "\"type\":\""
    val typeI = line.indexOf(strFlag)
    val sub = line.substring(typeI)
    val endI = sub.indexOf(",")
    sub.substring(strFlag.length, endI).replaceAll("\"", "")
  }

  def getDictColumn(keyStr: String) = {
    val stringToString = new mutable.HashMap[String, Type]()
    stringToString.+=(("int", Type.INT32))
    stringToString.+=(("bigint", Type.INT64))
    stringToString.+=(("double", Type.DOUBLE))
    stringToString.+=(("float", Type.FLOAT))
    stringToString.getOrElse(keyStr, Type.STRING)
  }

  def main(args: Array[String]): Unit = {
    val str = "{\"database\":\"db_investment\",\"table\":\"t_user_pay_record\",\"type\":\"insert\",\"ts\":1568791911,\"xid\":2951400881,\"commit\":true,\"data\":{\"inner_order\":\"GZG2019091813126334\",\"pay_order\":\"\",\"pay_date\":null,\"account_id\":56018,\"subject_type\":1,\"subject_title\":\"股掌柜-多空信号\",\"days\":0.00,\"total_money\":0.00,\"pay_type\":8,\"pay_time\":\"2019-09-18 07:31:51\",\"privi_start_day\":\"2019-09-18\",\"privi_end_day\":\"2019-09-19\",\"updatetime\":\"2019-09-18 07:31:51\",\"dua\":\"\",\"status\":1,\"check_status\":null,\"subject_id\":\"6\",\"advisor_id\":null,\"consumer_id\":null,\"buyerid\":null,\"openId\":null,\"consumer_phone_num\":\"\",\"create_time\":\"2019-09-18 07:31:51\",\"headimage\":\"https://realsscf.oss-cn-hangzhou.aliyuncs.com/productpicture/tkxh.png\",\"infinite\":0,\"order_amount\":0.00,\"application_id\":1,\"product_id\":\"TZ20180627135136\",\"deadline\":\"1天\",\"remark\":null,\"refund_amount\":0.00,\"refund_time\":\"0000-00-00 00:00:00\",\"clientOrderUUID\":null,\"offline_order\":null,\"open_mode\":null,\"term\":0,\"term_unit\":\"\",\"invoice_id\":0,\"befor_refund_status\":0,\"order_type\":0,\"refund_apply_date\":\"0000-00-00 00:00:00\",\"refund_id\":0}}"
    val jSONObject = JSON.parseObject(str)
    val nObject = jSONObject.getJSONObject("data")
    val ssss = nObject.getString("inner_order")
    println(ssss)
  }
}
