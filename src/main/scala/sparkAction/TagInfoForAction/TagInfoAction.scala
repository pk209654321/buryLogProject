package sparkAction.TagInfoForAction

import java.sql
import java.util.{Date, Properties}

import conf.ConfigurationManager
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import scalaUtil.StructUtil
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/**
  * ClassName TagInfoAction
  * Description TODO
  * Author lenovo
  * Date 2019/5/31 10:41
  **/
object TagInfoAction {
  private val DRIVER=ConfigurationManager.getProperty("pc.mysql.driver")
  private val URL=ConfigurationManager.getProperty("pc.mysql.url")
  private val USER=ConfigurationManager.getProperty("pc.mysql.user")
  private val PASSWORD=ConfigurationManager.getProperty("pc.mysql.password")
  private val TABLENAME=ConfigurationManager.getProperty("pc.mysql.tablename")
  def doTagInfoAction(spark: SparkSession,sqlWhere:String)={
    val dataFrame = spark.sql(s"select user_id from cdm.t_user_tags where ${sqlWhere} group by user_id")
    /*dataFrame.foreachPartition(it => {
      val listBuffer = new ListBuffer[Int]
      it.foreach(line => {
        listBuffer.+=(line.getAs[Int]("user_id"))
      })
    })*/
    import spark.implicits._
    val userSession = dataFrame.rdd.map(line => {
      val user_id = line.getAs[String]("user_id")
      val toInt = user_id.toInt
      TagBean(toInt, "session_id",new Date())
    })
    val re = userSession.repartition(1)
    val userSessionDF = userSession.toDF().repartition(1)
    //spark.createDataFrame(userSession,StructUtil.structTagInfo)
    val prop=new Properties()
    prop.setProperty("user",USER)
    prop.setProperty("password",PASSWORD)
    userSessionDF.write.mode(SaveMode.Append).jdbc(URL,TABLENAME,prop)
  }

  def doTagInfoAction2(spark: SparkSession,sqlWhere:String)={
    val dataFrame = spark.sql(s"select user_id from cdm.t_user_tags where ${sqlWhere} group by user_id")
    val userSession = dataFrame.rdd.map(line => {
      val user_id = line.getAs[String]("user_id")
      val toInt = user_id.toInt
      (toInt,"session_id",new Date())
    }).repartition(1)

    userSession.foreachPartition(it => {
      DBs.setupAll()
      val beans = it.map(line => {
        val user_id = line._1
        val session_id = line._2
        val create_time = line._3
        Seq(user_id, session_id, create_time)
      }).toArray
      NamedDB('tag).localTx {
        implicit session => SQL("insert into tag_info (user_id,session_id,create_time) values (?,?,?)").batch(beans:_*).apply()
      }
    })

  }
}

case class TagBean(user_id:Int,session_id:String,create_time:Date)
