package scalaUtil

import org.apache.spark.sql.types._

/**
  * Created by lenovo on 2018/11/16.
  */
object StructUtil {
  //按字段划分(弃用)
  val structClient = StructType(List(
    StructField("user_id", IntegerType),
    StructField("guid", StringType),
    StructField("application", StringType),
    StructField("version", StringType),
    StructField("platform", StringType),
    StructField("object", StringType),
    StructField("createtime", LongType),
    StructField("action_type", IntegerType),
    StructField("is_fanhui", IntegerType),
    StructField("scode_id", StringType),
    StructField("market_id", StringType),
    StructField("screen_direction", StringType),
    StructField("color", StringType),
    StructField("frameid", StringType),
    StructField("type", IntegerType),
    StructField("qs_id", StringType),
    StructField("from_frameid", StringType),
    StructField("from_object", StringType),
    StructField("from_resourceid", StringType),
    StructField("to_frameid", StringType),
    StructField("to_resourceid", StringType),
    StructField("to_scode", StringType),
    StructField("target_id", StringType)
  ))

  val structWeb = StructType(List(
    StructField("user_id", IntegerType),
    StructField("guid", StringType),
    StructField("application", StringType),
    StructField("version", StringType),
    StructField("platform", StringType),
    StructField("id", StringType),
    StructField("createtime", LongType),
    StructField("opentime", LongType),
    StructField("action_type", IntegerType),
    StructField("is_fanhui", IntegerType),
    StructField("scode_id", StringType),
    StructField("market_id", StringType),
    StructField("screen_direction", StringType),
    StructField("color", StringType),
    StructField("frameid", StringType),
    StructField("task_id", IntegerType),
    StructField("qs_id", StringType),
    StructField("from_frameid", StringType),
    StructField("from_object", StringType),
    StructField("from_resourceid", StringType),
    StructField("to_frameid", StringType),
    StructField("to_resourceid", StringType),
    StructField("to_scode", StringType),
    StructField("order_num", StringType),
    StructField("activity_id", StringType)
  ))

  val structVisit = StructType(List(StructField("user_id", IntegerType),
    StructField("guid", StringType),
    StructField("access_time", LongType),
    StructField("offline_time", LongType),
    StructField("download_channel", StringType),
    StructField("client_version", StringType),
    StructField("phone_model", StringType),
    StructField("phone_system", StringType),
    StructField("system_version", StringType),
    StructField("operator", StringType),
    StructField("network", StringType),
    StructField("resolution", StringType),
    StructField("screen_height", StringType),
    StructField("screen_width", StringType),
    StructField("mac", StringType),
    StructField("ip", StringType),
    StructField("imei", StringType),
    StructField("iccid", StringType),
    StructField("meid", StringType),
    StructField("idfa", StringType)
  ))

  //map类型划分
  val structClientMap = StructType(List(StructField("map_col", MapType(StringType, StringType))))
  val structWebMap = StructType(List(StructField("map_col", MapType(StringType, StringType))))
  val structVisitMap = StructType(List(StructField("map_col", MapType(StringType, StringType))))
  val structPhoneWebMap = StructType(List(StructField("map_col", MapType(StringType, StringType))))
  //map_ip类型划分
  val structCommonMapIp = StructType(List(StructField("map_col", MapType(StringType, StringType)),
    StructField("sip", StringType)
  ))
  //map_ip_map类型划分
  val structCommonMapIpMap = StructType(List(StructField("map_col", MapType(StringType, StringType)),
    StructField("sip", StringType),
    StructField("other_map", MapType(StringType, StringType))
  ))
  //string_ip_map类型划分
  val structCommonStringIpMap = StructType(List(StructField("log_str", StringType),
    StructField("sip", StringType),
    StructField("other_map", MapType(StringType, StringType))
  ))
  //string_ip类型划分
  val structCommonStringIp = StructType(List(StructField("log_str", StringType), StructField("sip", StringType)))
  //string_ip_time
  val structCommonStringIpTime=StructType(List(StructField("log_str", StringType), StructField("sip", StringType),StructField("send_time",StringType)))
  //portfolio类型划分
  val structPortfolio = StructType(List(StructField("sKey", StringType), StructField("sValue", StringType), StructField("updatetime", StringType)))
  val structPortfolioMany = StructType(
    List(
      StructField("bRecvAnnounce", BooleanType),
      StructField("bRecvResearch", BooleanType),
      StructField("fChipHighPrice", FloatType),
      StructField("fChipLowPrice", FloatType),
      StructField("fDecreasesPer", FloatType),
      StructField("fHighPrice", FloatType),
      StructField("fIncreasePer", FloatType),
      StructField("fLowPrice", FloatType),
      StructField("fMainChipHighPrice", FloatType),
      StructField("fMainChipLowPrice", FloatType),
      StructField("iCreateTime", IntegerType),
      StructField("iUpdateTime", IntegerType),
      StructField("iVersion", IntegerType),
      StructField("sAiAlert", BooleanType),
      StructField("sDel", BooleanType),
      StructField("sDKAlert", BooleanType),
      StructField("sDtSecCode", StringType),
      StructField("sHold", BooleanType),
      StructField("sKey", StringType),
      StructField("sName", StringType),
      StructField("stCommentInfo_iCreateTime", IntegerType),
      StructField("stCommentInfo_iUpdateTime", IntegerType),
      StructField("stCommentInfo_sComment", StringType),
      StructField("updateTime", StringType)
    ))
  val structPortfolioProSecInfo = StructType(
    List(
      StructField("bRecvAnnounce", BooleanType),
      StructField("bRecvResearch", BooleanType),
      StructField("fChipHighPrice", FloatType),
      StructField("fChipLowPrice", FloatType),
      StructField("fDecreasesPer", FloatType),
      StructField("fHighPrice", FloatType),
      StructField("fIncreasePer", FloatType),
      StructField("fLowPrice", FloatType),
      StructField("fMainChipHighPrice", FloatType),
      StructField("fMainChipLowPrice", FloatType),
      StructField("iCreateTime", IntegerType),
      StructField("iUpdateTime", IntegerType),
      StructField("iVersion", IntegerType),
      StructField("isAiAlert", BooleanType),
      StructField("isDel", BooleanType),
      StructField("isDKAlert", BooleanType),
      StructField("sDtSecCode", StringType),
      StructField("isHold", BooleanType),
      StructField("sKey", StringType),
      StructField("sName", StringType),
      StructField("stCommentInfo_iCreateTime", IntegerType),
      StructField("stCommentInfo_iUpdateTime", IntegerType),
      StructField("stCommentInfo_sComment", StringType),
      StructField("vBroadcastTime",ArrayType(IntegerType)),
      StructField("vStrategyId",ArrayType(IntegerType)),
      StructField("updateTime", StringType),
      StructField("lUptTimeExt", LongType),
      StructField("bInitiative", BooleanType)
    ))

  val structPortGroupInfo=StructType(List(
    StructField("gi_iCreateTime", IntegerType),
    StructField("gi_isDel", BooleanType),
    StructField("gi_iUpdateTime", IntegerType),
    StructField("gi_sGroupName", StringType),
    StructField("gs_isDel", BooleanType),
    StructField("gs_iUpdateTime", IntegerType),
    StructField("gs_sDtSecCode", StringType),
    StructField("iVersion", IntegerType),
    StructField("sKey", StringType),
    StructField("updateTime", StringType),
    StructField("gs_lUptTimeExt", LongType),
    StructField("gi_lUptTimeExt", LongType)
  ))

  val structShareMany=StructType(List(
    StructField("bgcolor", IntegerType),
    StructField("bgUrl", StringType),
    StructField("code", IntegerType),
    StructField("desc", StringType),
    StructField("downShareViewUrl", StringType),
    StructField("link", StringType),
    StructField("mb_content", StringType),
    StructField("mb_contentLink", StringType),
    StructField("mb_textColor", StringType),
    StructField("mb_type", StringType),
    StructField("middleChar", StringType),
    StructField("name", StringType),
    StructField("permission", IntegerType),
    StructField("person", IntegerType),
    StructField("posterChar", StringType),
    StructField("randomTitle", StringType),
    StructField("randomTitleConfigs", ArrayType(StringType)),
    StructField("sceneCode", IntegerType),
    StructField("shareImgUrl", StringType),
    StructField("shareTitle", StringType),
    StructField("shareType", IntegerType),
    StructField("topBlockLink", StringType),
    StructField("topBlockUrl", StringType),
    StructField("upShareViewUrl", StringType)
  ))

  val structEarlyWarn=StructType(List(
    StructField("iAccountId",LongType),
    StructField("vGUID",StringType),
    StructField("lUptTime",LongType),
    StructField("iSwitch",IntegerType),
    StructField("bLimitUp",BooleanType),
    StructField("bLimitDown",BooleanType),
    StructField("bSpeedUp",BooleanType),
    StructField("bSpeedDown",BooleanType),
    StructField("bChangeAsc",BooleanType),
    StructField("bChangeDesc",BooleanType),
    StructField("bDay30Highest",BooleanType),
    StructField("bDay60Highest",BooleanType),
    StructField("sDtSecCode",StringType),
    StructField("sDtSecName",StringType),
    StructField("dUpperPoint",DoubleType),
    StructField("dLowerPoint",DoubleType),
    StructField("dDayChangeAsc",DoubleType),
    StructField("dDayChangeDesc",DoubleType),
    StructField("dFiveMinChangeAsc",DoubleType),
    StructField("dFiveMinChangeDesc",DoubleType),
    StructField("vBroadcastTime",ArrayType(IntegerType)),
    StructField("vStrategyId",ArrayType(IntegerType)),
    StructField("dataKey",StringType),
    StructField("updateTime",LongType)
  ))

  //string_ip_map类型划分
  val structTagInfo = StructType(List(StructField("user_id", IntegerType),
    StructField("session_id", StringType)
  ))

  val structPushButton=StructType(List(StructField("iAccountId",LongType),
    StructField("mMsgPushButton",MapType(IntegerType,IntegerType)),
      StructField("updatetime",StringType)
  ))

  val structAnswer=StructType(List(
    StructField("id",LongType),
    StructField("user_id",StringType),
    StructField("ans_array",ArrayType(StringType)),
    StructField("update_time",LongType)
  ))
}
