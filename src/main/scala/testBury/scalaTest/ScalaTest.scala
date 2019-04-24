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
      SQL(s"select * from nf_user_intelligent_data").map(rs => {
        val keyStr = rs.string("DATA_KEY")
        val valueStr = rs.bytes("DATA_VALUE")
        val timeStr = rs.long("UPDATE_TIME")
        val stream = new BaseDecodeStream(valueStr)
        val userStockAlertCfgData = new UserStockAlertCfgData()
        userStockAlertCfgData.readFrom(stream)
        println(JSON.toJSONString(userStockAlertCfgData, SerializerFeature.WriteMapNullValue))
      }).list().apply()
    }
  }
}
