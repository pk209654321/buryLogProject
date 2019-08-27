package testBury.scalaTest


import bean.selectStock.ProSecInfoList
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.qq.tars.protocol.tars.BaseDecodeStream
import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}

/**
  * ClassName TempTest
  * Description TODO 获取88测试库db_op数据
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object ScalaTest_db_op {


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
    val peoples = NamedDB('op).readOnly { implicit session =>
      SQL(s"select * from t_portfolio where sKey='PROFO:96158'").map(rs => {
        val keyStr = rs.string("sKey")
        val valueStr = rs.bytes("sValue")
        val timeStr = rs.string("updatetime")
        val stream = new BaseDecodeStream(valueStr)
        val list = new ProSecInfoList()
        list.readFrom(stream)
        val sValue = JSON.toJSONString(list, SerializerFeature.WriteMapNullValue)
        println(sValue)
        sValue
      }).list().apply()
    }
    peoples
  }
}
