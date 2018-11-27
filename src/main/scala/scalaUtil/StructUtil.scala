package scalaUtil

import org.apache.spark.sql.types._

/**
  * Created by lenovo on 2018/11/16.
  */
object StructUtil {
  val structClient =StructType(List(
    StructField("user_id",IntegerType),
    StructField("guid",StringType),
    StructField("application",StringType),
    StructField("version",StringType),
    StructField("platform",StringType),
    StructField("object",StringType),
    StructField("createtime",LongType),
    StructField("action_type",IntegerType),
    StructField("is_fanhui",IntegerType),
    StructField("scode_id",StringType),
    StructField("market_id",StringType),
    StructField("screen_direction",StringType),
    StructField("color",StringType),
    StructField("frameid",StringType),
    StructField("type",IntegerType),
    StructField("qs_id",StringType),
    StructField("from_frameid",StringType),
    StructField("from_object",StringType),
    StructField("from_resourceid",StringType),
    StructField("to_frameid",StringType),
    StructField("to_resourceid",StringType),
    StructField("to_scode",StringType),
    StructField("target_id",StringType)
  ))

  val structWeb=StructType(List(
    StructField("user_id",IntegerType),
    StructField("guid",StringType),
    StructField("application",StringType),
    StructField("version",StringType),
    StructField("platform",StringType),
    StructField("id",StringType),
    StructField("createtime",LongType),
    StructField("opentime",LongType),
    StructField("action_type",IntegerType),
    StructField("is_fanhui",IntegerType),
    StructField("scode_id",StringType),
    StructField("market_id",StringType),
    StructField("screen_direction",StringType),
    StructField("color",StringType),
    StructField("frameid",StringType),
    StructField("task_id",IntegerType),
    StructField("qs_id",StringType),
    StructField("from_frameid",StringType),
    StructField("from_object",StringType),
    StructField("from_resourceid",StringType),
    StructField("to_frameid",StringType),
    StructField("to_resourceid",StringType),
    StructField("to_scode",StringType),
    StructField("order_num",StringType),
    StructField("activity_id",StringType)
  ))

  val structVisit=StructType(List(StructField("user_id",IntegerType),
    StructField("guid",StringType),
    StructField("access_time",LongType),
    StructField("offline_time",LongType),
    StructField("download_channel",StringType),
    StructField("client_version",StringType),
    StructField("phone_model",StringType),
    StructField("phone_system",StringType),
    StructField("system_version",StringType),
    StructField("operator",StringType),
    StructField("network",StringType),
    StructField("resolution",StringType),
    StructField("screen_height",StringType),
    StructField("screen_width",StringType),
    StructField("mac",StringType),
    StructField("ip",StringType),
    StructField("imei",StringType),
    StructField("iccid",StringType),
    StructField("meid",StringType),
    StructField("idfa",StringType)
  ))
}
