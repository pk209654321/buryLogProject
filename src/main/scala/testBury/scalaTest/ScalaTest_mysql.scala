package testBury.scalaTest

import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}

/**
  * ClassName TempTest
  * Description TODO 获取88测试库db_op数据
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object ScalaTest_mysql {


  def main(args: Array[String]): Unit = {
    DBs.setupAll()
    val peoples = NamedDB('kuduIm).autoCommit { implicit session =>
      SQL(s"select inner_order from kudu_real.t_user_pay_record limit 10").map(rs => {
        val db = rs.get[String]("inner_order")
        println("===================="+db)
      }).list().apply()
    }
  }

  def getEarlyWarningTest() = {
    DBs.setupAll()
    val res=NamedDB('hbase).readOnly { implicit session =>
      SQL(s"select * from t_user_pay_record limit 0,10000").map(rs => {
       Seq(rs.get[String]("inner_order"),
         rs.get[String]("pay_order"),
         rs.get[String]("pay_date"),
         rs.get[String]("account_id"),
         rs.get[String]("subject_type"),
         rs.get[String]("subject_title"),
         rs.get[String]("days"),
         rs.get[String]("total_money"),
         rs.get[String]("pay_type"),
         rs.get[String]("pay_time"),
         rs.get[String]("privi_start_day"),
         rs.get[String]("privi_end_day"),
         rs.get[String]("updatetime"),
         rs.get[String]("dua"),
         rs.get[String]("status"),
         rs.get[String]("check_status"),
         rs.get[String]("subject_id"),
         rs.get[String]("advisor_id"),
         rs.get[String]("consumer_id"),
         rs.get[String]("buyerid"),
         rs.get[String]("openId"),
         rs.get[String]("consumer_phone_num"),
         rs.get[String]("create_time"),
         rs.get[String]("headimage"),
         rs.get[String]("infinite"),
         rs.get[String]("order_amount"),
         rs.get[String]("application_id"),
         rs.get[String]("product_id"),
         rs.get[String]("deadline"),
         rs.get[String]("remark"),
         rs.get[String]("refund_amount"),
         rs.get[String]("refund_time"),
         rs.get[String]("clientOrderUUID"),
         rs.get[String]("offline_order"),
         rs.get[String]("open_mode"),
         rs.get[String]("term"),
         rs.get[String]("term_unit"),
         rs.get[String]("invoice_id"),
         rs.get[String]("befor_refund_status"),
         rs.get[String]("order_type"),
         rs.get[String]("refund_apply_date"),
         rs.get[String]("refund_id")
        )
      }).list().apply()
    }
    res
  }

  def getMysqlDataNew(diffDay: Int) = {
    DBs.setupAll()
    val peoples = NamedDB('tohbase).autoCommit { implicit session =>
      SQL(s"select * from hera_table_info").map(rs => {
        val db = rs.get[String]("table_schema")
        val tb1 = rs.get[String]("table_name1")
        val tb2 = rs.get[String]("table_name2")
        (db,tb1,tb2)
      }).list().apply()
    }
  }

  def getKudu(): Unit ={

  }
}
