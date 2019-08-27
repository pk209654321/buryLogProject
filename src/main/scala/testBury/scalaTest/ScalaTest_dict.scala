package testBury.scalaTest


import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}

/**
  * ClassName TempTest
  * Description TODO 获取88测试库db_op数据
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object ScalaTest_dict {


  def main(args: Array[String]): Unit = {
    getMysqlDataNew(-1)
  }

  def getEarlyWarningTest(): Unit = {
    DBs.setupAll()
    NamedDB('warn).readOnly { implicit session =>
      SQL(s"select * from db_sscf.t_notify_push_button limit 10").map(rs => {
        val b = rs.get[Array[Byte]]("sValue")
        val str = new String(b)
        println(str)
      }).list().apply()
    }
  }

  def getMysqlDataNew(diffDay: Int) = {
    DBs.setupAll()
    val peoples = NamedDB('dict_table).readOnly { implicit session =>
      SQL(s"select * from hera_table_info").map(rs => {
        val db = rs.get[String]("table_schema")
        val tb1 = rs.get[String]("table_name1")
        val tb2 = rs.get[String]("table_name2")
        (db,tb1,tb2)
      }).list().apply()
    }
    peoples.map(line=>{
      NamedDB('dict_table).autoCommit { implicit session =>
        SQL("update hera_data_dict set table_name1 = ? where table_schema = ? and table_name2=?").bind(line._2,line._1,line._3).update().apply()
      }
    })
  }
}
