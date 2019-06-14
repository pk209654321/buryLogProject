package sparkAction.portfolioHive

import java.util

import com.niufu.tar.bec.SetAccountPushButtonReq
import com.qq.tars.protocol.tars.BaseDecodeStream
import conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import scalaUtil.StructUtil
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs

import scala.collection.{JavaConversions, mutable}

/**
  * ClassName UserPushButtonInsertToHive
  * Description TODO
  * Author lenovo
  * Date 2019/6/5 14:38
  **/
object UserPushButtonInsertToHive {

  private val TABLE: String = ConfigurationManager.getProperty("notifyPushButton")
  def doUserPushButtonInsertToHive( userPushRdd: RDD[(Long, mutable.Map[Integer, Integer], String)],spark: SparkSession): Unit ={
    val userPushRow = userPushRdd.map(line => {
      val iAccountId = line._1
      val mMsgPushButton = line._2
      val updatetime = line._3
      Row(iAccountId, mMsgPushButton, updatetime)
    })
    val dataFream = spark.createDataFrame(userPushRow,StructUtil.structPushButton).repartition(1)
    dataFream.createOrReplaceTempView("tempTable")
    spark.sql(s"insert overwrite  table ${TABLE} select * from tempTable")
  }

  //从中间mysql库中获取数据
  def getUserPushData()={
    DBs.setupAll()
    val tempData = NamedDB('button).readOnly { implicit session =>
      SQL(s"select * from t_notify_push_button").map(rs => {
        val sKey = rs.string("sKey")
        val sValue = rs.bytes("sValue")
        val updatetime = rs.string("updatetime")
        val stream = new BaseDecodeStream(sValue)
        val setAccountPushButtonReq = new SetAccountPushButtonReq()
        setAccountPushButtonReq.readFrom(stream)
        val iAccountId = setAccountPushButtonReq.getIAccountId
        val mMsgPushButton = setAccountPushButtonReq.getMMsgPushButton
        mMsgPushButton match {
          case null =>(iAccountId,null,updatetime)
          case _ =>(iAccountId,JavaConversions.mapAsScalaMap(mMsgPushButton),updatetime)
        }
      }).list().apply()
    }
    tempData
  }
}
