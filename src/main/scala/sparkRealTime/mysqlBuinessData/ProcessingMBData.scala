package sparkRealTime.mysqlBuinessData

import java.util
import java.util.ArrayList

import com.alibaba.fastjson.{JSON, JSONObject}
import hadoopCode.kudu.{KuduAgent, KuduColumn, KuduRow, KuduUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.Type
import org.apache.kudu.client.{KuduSession, KuduTable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.client
import scalaUtil.DateScalaUtil
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable

/**
  * ClassName ProcessingMBData
  * Description TODO
  * Author lenovo
  * Date 2019/9/18 17:29
  **/
object ProcessingMBData {
  def doProcessingMBData(oneRdd: RDD[(String, String)], db_s: String, tb_s: String, db_t: String, tb_t: String, pk: String, kuduTb: String): Unit = {
    val filterData = oneRdd.map(_._2).filter(line => {
      val temp1 = "\"database\":\"" + db_s + "\",\"table\":\"" + tb_s + "\""
      val idml = line.indexOf(temp1)
      if (idml > -1) true else false
    })

    filterData.foreachPartition(it => {
      val session = KuduUtils.getSession
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
          if(StringUtils.isNotBlank(pay_time)){
            json.put("pay_time", DateScalaUtil.getAddEight(0, pay_time, 8))
          }
          if(StringUtils.isNotBlank(updatetime)){
            json.put("updatetime", DateScalaUtil.getAddEight(0, updatetime, 8))
          }
          if(StringUtils.isNotBlank(create_time)){
            json.put("create_time", DateScalaUtil.getAddEight(0, create_time, 8))
          }
          if(StringUtils.isNotBlank(refund_time)){
            json.put("refund_time", DateScalaUtil.getAddEight(0, refund_time, 8))
          }
          if(StringUtils.isNotBlank(refund_apply_date)){
            json.put("refund_apply_date", DateScalaUtil.getAddEight(0, refund_apply_date, 8))
          }
        }else{
          val sqlStr = jSONObject.getString("sql")
          println("------------------------------------------修改语句"+sqlStr)
        }
        typeStr match {
          case "insert" => doUpsert3(line, kuduTb, session, json, pk)
          case "update" => doUpsert3(line, kuduTb, session, json, pk)
          case "delete" => doDelete3(line, kuduTb, session, json, pk)
          case "table-alter" => doDDL3(line, tb_s, kuduTb)
          case _ =>
        }
      })
      session.flush()
      KuduUtils.closeSession()
    })
  }

  def doSql(sql: String): Unit = {
    NamedDB('kuduIm).autoCommit { implicit session =>
      SQL(sql).update().apply()
    }
  }

  def kuduInsert() {
    val agent = new KuduAgent
  }


  def doUpsert3(line: String, tableName: String, session: KuduSession, json: JSONObject, pk: String) {
    val upsert = KuduUtils.createUpsert(tableName, json)
    session.apply(upsert)
  }

  def doDelete3(line: String, tableName: String, session: KuduSession, json: JSONObject, pk: String) {
    val delete = KuduUtils.createDelete(tableName, json, pk)
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
    doSql(sql)
  }

  def getColType(oneDDL: String) = {
    val iS = oneDDL.indexOf("  ")
    val eS = oneDDL.substring(iS).indexOf(" ")
    val colType = oneDDL.substring(iS + 2, eS)
    val im_colt = getDictColumn(colType)
    im_colt
  }


  def doDDL3(line:String,tb_s:String,table_name:String) {
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
        KuduUtils.alterTableAddColumn(table_name,coln,im_colt)
      }

      if (dropF > -1) {
        KuduUtils.alterTableDeleteColumn(table_name,coln)
      }

      if (changeF > -1) {
        val newName = local(3)
        KuduUtils.alterTableChangeColumn(table_name,coln,newName)
      }
    }
  }


  def doDDL(line: String, db_t: String, tb_t: String) = {
    println("ddl=====" + line)
    val sqlStr = "\"sql\":\"ALTER TABLE `" + tb_t + "`"
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
        val sql = s"alter table ${db_t}.${tb_t} add columns (${coln} ${im_colt})"
        println(sql)
      }

      if (dropF > -1) {
        val sql = s"alter table ${db_t}.${tb_t} drop column ${coln}"
        println(sql)
      }

      if (changeF > -1) {
        val str3 = local(3)
        val str5 = local(5)
        val im_colt = getDictColumn(str5)
        val sql = s"alter table ${db_t}.${tb_t} change column ${coln} ${str3} ${im_colt}}"
        println(sql)
      }
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

  def doDelete(line: String, db_t: String, tb_t: String, pk: String) = {
    println("delete=====" + line)
    val pks = pk.split(",")
    var sqlWhere = ""
    for (index <- (0 until (pks.length))) {
      val valStr = getValByKey(pks(index), line)
      if (index == pks.length - 1) {
        sqlWhere += pks(index) + "=" + valStr
      } else {
        sqlWhere += pks(index) + "=" + valStr + " and "
      }
    }
    var sql = s"delete from ${db_t}.${tb_t} where ${sqlWhere}"
    if (sqlWhere.equals("")) {
      sql = ""
    } else {
      sql
    }
    doSql(sql)
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
