package sparkAction.polarLightForAction

import conf.ConfigurationManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs

/**
  * ClassName PolarLightAction
  * Description 极光对接逻辑
  * Author lenovo
  * Date 2019/5/17 10:52
  **/
object PolarLightAction {
  private val DRIVER: String = ConfigurationManager.getProperty("aurora.mysql.driver")
  private val URL: String = ConfigurationManager.getProperty("aurora.mysql.url")
  private val USER: String = ConfigurationManager.getProperty("aurora.mysql.user")
  private val PASSWORD: String = ConfigurationManager.getProperty("aurora.mysql.password")
  private val TABLENAME: String = ConfigurationManager.getProperty("aurora.mysql.tablename")


  private val DRIVER1: String = ConfigurationManager.getProperty("register.mysql.driver")
  private val URL1: String = ConfigurationManager.getProperty("register.mysql.url")
  private val USER1: String = ConfigurationManager.getProperty("register.mysql.user")
  private val PASSWORD1: String = ConfigurationManager.getProperty("register.mysql.password")
  private val TABLENAME1: String = ConfigurationManager.getProperty("register.mysql.tablename")

  def polarLightForActive(spark: SparkSession) = {
    /**
      * 　　* @Description: TODO 清洗出手机客户端的数据insert到hive仓库中
      * 　　* @param [filterData, hc, dayFlag]
      * 　　* @return org.apache.spark.sql.DataFrame
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/2/12 13:45
      * 　　*/
    //读取极光激活历史数据(已经推送给了极光)
    spark.read.format("jdbc")
      .option("url", URL)
      .option("driver", DRIVER)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("dbtable", s"(select * from ${TABLENAME} where conv_type='APP_ACTIVE') as t1")
      .load().createOrReplaceTempView("aurora_mysql")
    //读取实时激活数据
    spark.sql(PolarLigthSql.hql).createOrReplaceTempView("aurora_hive")
    //过滤历史数据推送新数据(where m.device_id is null)
    val tempSql =
      """
        |select
        |h.guid,
        |h.device_id,
        |h.device_type,
        |h.conv_type,
        |h.access_time,
        | h.hp_stat_date
        | from aurora_hive as h left join aurora_mysql as m
        | on h.device_id=m.device_id
        |where m.device_id is null
      """.stripMargin
    spark.sql(tempSql).repartition(1)
      .foreachPartition(it => {
        DBs.setupAll()
        it.foreach(line => {
          try {
            val guid = line.getAs[String]("guid")
            val device_id = line.getAs[String]("device_id")
            val device_type = line.getAs[String]("device_type")
            val conv_type = line.getAs[String]("conv_type")
            val access_time = line.getAs[String]("access_time")
            val hp_stat_date = line.getAs[String]("hp_stat_date")
            val i = NamedDB('aurora).autoCommit {
              val insertSql =
                s"""
insert into ${TABLENAME} (
guid,
device_id,
device_type,
conv_type,
access_time,
hp_stat_date)
values (
?,?,?,?,?,?
)
                """.stripMargin
              implicit session => SQL(insertSql).bind(guid, device_id, device_type, conv_type, access_time, hp_stat_date).update().apply()
            }
            if (i > 0) {
              //insert成功开始发送数据
              println(s"激活数据开始发送--------device_id:${device_id}-----device_type:${device_type}-----conv_type:${conv_type}-----access_time:${access_time}")
              PolarLightPost.postPolarLight(PolarLightBean(device_id, device_type, conv_type))
            } else {
              //insert失败
              println(s"激活数据insert失败-------device_id:${device_id}-----device_type:${device_type}-----conv_type:${conv_type}-----access_time:${access_time}")
            }
          } catch {
            case e:Throwable => e.printStackTrace()
          }
        })
      })
  }

  def polarLightForRegister(spark: SparkSession) = {
    /**
      * 　　* @Description: TODO 清洗出手机客户端的数据insert到hive仓库中
      * 　　* @param [filterData, hc, dayFlag]
      * 　　* @return org.apache.spark.sql.DataFrame
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/2/12 13:45
      * 　　*/

    //读取极光历史注册数据(已经推送给极光)
    spark.read.format("jdbc")
      .option("url", URL)
      .option("driver", DRIVER)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("dbtable", s"(select * from ${TABLENAME} where conv_type='APP_REGISTER') as t1")
      .load().createOrReplaceTempView("aurora_mysql_register")

    //读取极光历史激活数据(已经推送给极光)
    spark.read.format("jdbc")
      .option("url", URL)
      .option("driver", DRIVER)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("dbtable", s"(select * from ${TABLENAME} where conv_type='APP_ACTIVE') as t1")
      .load().createOrReplaceTempView("aurora_mysql_active")
    //读取近两天注册数据(已经推送给极光)
    spark.read.format("jdbc")
      .option("url", URL1)
      .option("driver", DRIVER1)
      .option("user", USER1)
      .option("password", PASSWORD1)
      .option("dbtable", s"(select sGUID from t_account_resigter_info WHERE  time <=unix_timestamp(CURDATE()) AND time >= UNIX_TIMESTAMP(DATE_SUB(curdate(),INTERVAL 1 DAY)) and sGUID <> '' and sPhone <> '') as t1")
      .load().createOrReplaceTempView("register_mysql")

    //根据注册表先->过滤注册历史数据->匹配激活历史数据->推送新注册数据
    val tempSql =
      """
        | select ama.guid,
        | ama.device_id,
        | ama.device_type,
        | ama.conv_type,
        | ama.access_time,
        | ama.hp_stat_date
        |  from  (select r.sGUID from register_mysql r left join
        |aurora_mysql_register a
        |on r.sGUID = a.guid
        |where a.guid is null) t1
        |inner Join aurora_mysql_active ama
        |on t1.sGUID=ama.guid
      """.stripMargin
    spark.sql(tempSql).repartition(1)
      .foreachPartition(it => {
        DBs.setupAll()
        it.foreach(line => {
          try {
            val guid = line.getAs[String]("guid")
            val device_id = line.getAs[String]("device_id")
            val device_type = line.getAs[String]("device_type")
            val conv_type = "APP_REGISTER"
            val access_time = line.getAs[String]("access_time")
            val hp_stat_date = line.getAs[String]("hp_stat_date")
            val i = NamedDB('aurora).autoCommit {
              val insertSql =
                s"""
insert into ${TABLENAME} (
guid,
device_id,
device_type,
conv_type,
access_time,
hp_stat_date)
values (
?,?,?,?,?,?
)
                """.stripMargin
              implicit session => SQL(insertSql).bind(guid, device_id, device_type, conv_type, access_time, hp_stat_date).update().apply()
            }
            if (i > 0) {
              //insert成功开始发送数据
              println(s"注册数据开始发送--------device_id:${device_id}-----device_type:${device_type}-----conv_type:${conv_type}-----access_time:${access_time}")
              PolarLightPost.postPolarLight(PolarLightBean(device_id, device_type, conv_type))
            } else {
              //insert失败
              println(s"激活数据insert失败-------device_id:${device_id}-----device_type:${device_type}-----conv_type:${conv_type}-----access_time:${access_time}")
            }
          } catch {
            case e:Throwable => e.printStackTrace()
          }
        })
      })
  }
}

case class PolarLightBean(device_id: String, device_type: String, conv_type: String)
