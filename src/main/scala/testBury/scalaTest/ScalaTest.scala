package testBury.scalaTest

import java.util.List

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.niufu.tar.bec.{StockIntelligentAlert, UserStockAlertCfgData}
import com.qq.tars.protocol.tars.BaseDecodeStream
import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}

/**
  * ClassName TempTest
  * Description TODO
  * Author lenovo
  * Date 2018/12/29 10:50
  **/
object ScalaTest {


  def main(args: Array[String]): Unit = {
    getEarlyWarningTest
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
}
