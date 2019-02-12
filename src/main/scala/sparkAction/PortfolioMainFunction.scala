package sparkAction

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.dengtacj.bec.ProSecInfoList
import com.qq.tars.protocol.tars.BaseDecodeStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scalaUtil.{LocalOrLine, MailUtil}
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs
import sparkAction.portfolioHive.PortfolioHiveInsertObject

/**
  * ClassName PortfolioMainFunction
  * Description TODO
  * Author lenovo
  * Date 2019/2/12 9:16
  **/
object PortfolioMainFunction {
  def main(args: Array[String]): Unit = {
    try {
      val portfolioStrs = getPortfolioFromMysql()
      val diffDay: Int = args(0).toInt
      val local: Boolean = LocalOrLine.judgeLocal()

      var sparkConf: SparkConf = new SparkConf().setAppName("BuryMainFunction")
      if (local) {
        //System.setProperty("HADOOP_USER_NAME", "wangyd")
        sparkConf = sparkConf.setMaster("local[*]")
      }
      val sc: SparkContext = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      val hc: HiveContext = new HiveContext(sc)
      //for (dayFlag <- (diffDay to -1)) { //按天数循环
      val portfolias = sc.parallelize(portfolioStrs ,1)
      PortfolioHiveInsertObject.insertPortfolioToHive(portfolias,hc,diffDay)
      //}
      sc.stop()
    } catch {
      case e: Throwable => e.printStackTrace();MailUtil.sendMail("spark用户自选股解析入库", "失败")
    }
  }

  def getPortfolioFromMysql:List[PortfolioStr] ={
    DBs.setupAll()
    val peoples = NamedDB('mysql).readOnly { implicit session =>
      SQL("select * from t_portfolio").map(rs => Portfolio(rs.string("sKey"),rs.bytes("sValue"),rs.string("updatetime"))).list().apply()
    }
    val portfolioStrs = peoples.map(one => {
      val value = one.sValue
      val stream = new BaseDecodeStream(value)
      val list = new ProSecInfoList()
      list.readFrom(stream)
      val sValue = JSON.toJSONString(list, SerializerFeature.WriteMapNullValue)
      PortfolioStr(one.sKey, sValue, one.updatetime)
    })
    portfolioStrs
  }
}

case class Portfolio (sKey:String,sValue:Array[Byte],updatetime:String)
case class PortfolioStr(sKey:String,sValue:String,updatetime:String)
