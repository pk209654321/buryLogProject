package sparkRealTime.touTiaoBackRealTime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import scalaUtil.{ExceptionMsgUtil, HttpRequest, MailUtil, Md5SumUtil}
import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}
import sparkAction.BuryLogin
import sparkAction.buryCleanUtil.BuryCleanCommon

/**
  * ClassName DataProcessingForTouTiao
  * Description TODO 过滤出登录日志,解析出设备详情匹配监测链接
  * Author lenovo
  * Date 2019/10/13 13:55
  **/
object DataProcessingForTouTiao {

  // TODO:
  def doDataProcessingForTouTiao(oneRdd: RDD[(String, String)]) {
    val valRdd = oneRdd.map(_._2)
    val filterBlank: RDD[String] = valRdd.filter(line => {
      //过滤为空的和有ip但是post传递为空的
      StringUtils.isNotBlank(line) && StringUtils.isNotBlank(line.split("&")(0))
    })
    //清洗去掉不规则字符并转换为list形式
    val allData = filterBlank.map(BuryCleanCommon.cleanCommonToListBuryLogin).filter(_.size() > 0)
    val rddOneObjcet: RDD[AnyRef] = allData.flatMap(_.toArray())
    //过滤空的line
    val allDataOneRdd = rddOneObjcet.map(_.asInstanceOf[BuryLogin]).filter(one => {
      val line = one.line
      StringUtils.isNotBlank(line) && one.logType == 1 && line.split("\\|", -1).size >= 23
    })
    val phoneInfoRDD = allDataOneRdd.map(one => {
      try {
        val lineStr = one.line
        val splits = lineStr.split("\\|", -1)
        //用户id
        val user_id = splits(0)
        //guid
        val guid = splits(1).trim
        //手机操作系统
        val phone_system = splits(7).trim
        //mac地址
        val mac = splits(14).trim
        //安卓设备信息
        val imei = splits(15).trim
        //ios设备信息
        val idfa = splits(18).trim
        //安卓唯一id
        val androidid = splits(22).trim

        (phone_system, androidid, imei, idfa, guid)
      } catch {
        case e: Exception => println(s"DataProcessingForTouTiao_error_log:logType=${one.logType},source=${one.source},line=${one.line}"); ("", "", "", "", "")
      }
    })
    //去重
    val disRdd = phoneInfoRDD.distinct()
    disRdd.foreachPartition(par => {
      // TODO: 加载数据库连接,加载近7天的监测数据
      //DBs.setupAll()
      DBs.setup('db_sscf)
      DBs.setup('phpmanager)
      DBs.setup('db_user)
      try {
        // TODO: 加载头条监测数据
        val jcData = NamedDB('db_sscf).readOnly {
          implicit session =>
            SQL(s"SELECT os,androidid,imei,idfa,callback_param FROM monitor_link_info WHERE DATE_ADD(CURDATE(),INTERVAL -2 DAY) <= create_time").map(rs => {
              ((rs.get[String]("os"),
                rs.get[String]("androidid"),
                rs.get[String]("imei"),
                rs.get[String]("idfa")
              ),
                rs.get[String]("callback_param"))
            }).list().apply()
        }

        // TODO: 加载已经激活发送的历史数据
        val hisData = NamedDB('phpmanager).readOnly {
          implicit session =>
            SQL(s"SELECT os,androidid,imei,idfa,callback_param FROM monitor_link_info_his").map(rs => {
              ((rs.get[String]("os"),
                rs.get[String]("androidid"),
                rs.get[String]("imei"),
                rs.get[String]("idfa")
                //
              ),
                rs.get[String]("callback_param"))
            }).list().apply()
        }

        // TODO: 加载注册表
        val registerData = NamedDB('db_user).readOnly {
          implicit session =>
            SQL(s"SELECT guid FROM t_account_resigter_info where create_time >= CURDATE()").map(rs => {
              rs.get[String]("guid")
            }).list().apply()
        }
        // TODO: 加载已经注册的发送的历史数据
        val registerDataHis = NamedDB('phpmanager).readOnly {
          implicit session =>
            SQL(s"SELECT guid FROM monitor_link_info_his_register ").map(rs => {
              rs.get[String]("guid")
            }).list().apply()
        }

        // TODO: 将注册表中的数据去除历史数据
        val guidDiff = registerData.diff(registerDataHis).toSet
        // TODO: 将检测数据去重并删除历史数据删除
        val jcDataNew = (jcData.toMap -- hisData.toMap.keySet).toList
        //val newData = jcData.toSet.diff(hisData.toSet).toList
        analysisPartition(par, jcDataNew, jcData.toMap.toList, guidDiff)
      } finally {
        DBs.close('db_sscf)
        DBs.close('phpmanager)
        DBs.close('db_user)
      }
    })
  }


  def analysisPartition(par: scala.Iterator[(String, String, String, String, String)]
                        , jcDataNew: scala.List[((String, String, String, String), String)]
                        , jcData: scala.List[((String, String, String, String), String)]
                        , guidDiff: Set[String]) {
    /**
      * 　　* @Description: TODO
      * 　　* @param [par, newData]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/10/13 20:54
      * 　　*/
    for (elem <- par) {
      val phone_system = elem._1
      var anid = elem._2
      var imei = elem._3
      val idfa = elem._4
      val guid = elem._5
      anid = Md5SumUtil.getMd5Sum(anid)
      imei = Md5SumUtil.getMd5Sum(imei)
      //val parOne = (anid, imei, idfa)
      //      println("日志log===========" + elem)
      // TODO: 1,激活上报
      for (jc <- jcDataNew) {
        val jcOne = jc._1
        val jc_os = jc._1._1
        val jc_anid = jc._1._2
        val jc_imei = jc._1._3
        val jc_idfa = jc._1._4
        if (jc_os.equals("0")) { //安卓
          if ((StringUtils.isNoneBlank(jc_imei) && jc_imei.equals(imei) && !jc_imei.equals("5284047f4ffb4e04824a2fd1d1f0cd62")) ||
            ((jc_imei.equals("5284047f4ffb4e04824a2fd1d1f0cd62") || StringUtils.isBlank(jc_imei)) && StringUtils.isNotBlank(jc_anid) && jc_anid.equals(anid))) {
            insertAndPush(jc, 0)
          }
        } else if (jc_os.equals("1")) { //ios
          if (StringUtils.isNotBlank(jc_idfa) && !jc_idfa.equals("00000000-0000-0000-0000-000000000000") && jc_idfa.equals(idfa)) {
            insertAndPush(jc, 0)
          }
        }
      }

      // TODO: 2,注册上报
      for (jcd <- jcData) {
        val jcd_os = jcd._1._1
        val jcd_anid = jcd._1._2
        val jcd_imei = jcd._1._3
        val jcd_idfa = jcd._1._4
        if (guidDiff.contains(guid)) { //判断guid是否在过滤后的注册表中
          if (jcd_os.equals("0")) { //安卓
            if ((StringUtils.isNoneBlank(jcd_imei) && !jcd_imei.equals("5284047f4ffb4e04824a2fd1d1f0cd62") && jcd_imei.equals(imei)) ||
              ((jcd_imei.equals("5284047f4ffb4e04824a2fd1d1f0cd62") || StringUtils.isBlank(jcd_imei))
                && StringUtils.isNotBlank(jcd_anid) && jcd_anid.equals(anid))) {
              insertAndPushRegister(jcd, 1, guid)
            }
          } else if (jcd_os.equals("1")) { //ios
            if (StringUtils.isNotBlank(jcd_idfa) && !jcd_idfa.equals("00000000-0000-0000-0000-000000000000") && jcd_idfa.equals(idfa)) {
              insertAndPushRegister(jcd, 1, guid)
            }
          }
        }
      }
    }
  }

  def insertAndPushRegister(his: ((String, String, String, String), String), event_type: Int, guid: String) {
    /**
      * 　　* @Description: TODO push(回调注册)数据 && insert到push历史table中
      * 　　* @param [jc]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/10/13 20:54
      * 　　*/
    val url = "http://ad.toutiao.com/track/activate/"
    var param = ""
    var muid = ""
    val mac = his._1._2
    val imei = his._1._3
    val idfa = his._1._4
    val callback_param = his._2
    val os = his._1._1
    os match {
      case "0" => muid = imei
      case "1" => muid = idfa
      case _ =>
    }
    param = s"muid=${muid}&os=${os}&callback=${callback_param}&event_type=${event_type}"
    try {
      val insertFlag = NamedDB('phpmanager).localTx {
        implicit session =>
          SQL("insert INTO monitor_link_info_his_register (guid) VALUES (?)").bind(guid).update().apply()
      }
      if (insertFlag > 0) {
        val backResult = HttpRequest.sendGet2(url, param)
        println(his + "======table monitor_link_info_his_register insert成功")
        println(backResult + "======table monitor_link_info_his_register insert成功")
      } else {
        println(his + "======table monitor_link_info_his_register insert失败")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def insertAndPush(jc: ((String, String, String, String), String), event_type: Int) {
    /**
      * 　　* @Description: TODO push(回调)数据 && insert到push历史table中
      * 　　* @param [jc]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/10/13 20:54
      * 　　*/
    val url = "http://ad.toutiao.com/track/activate/"
    var param = ""
    var muid = ""
    val androidid = jc._1._2
    val imei = jc._1._3
    val idfa = jc._1._4
    val callback_param = jc._2
    val os = jc._1._1
    os match {
      case "0" => muid = imei
      case "1" => muid = idfa
      case _ =>
    }
    param = s"muid=${muid}&os=${os}&callback=${callback_param}&event_type=${event_type}"


    try {

      val insertFlag = NamedDB('phpmanager).localTx {
        implicit session =>
          SQL("insert INTO monitor_link_info_his (os,androidid,imei,idfa,callback_param) VALUES (?,?,?,?,?)").bind(os, androidid, imei, idfa, callback_param).update().apply()
      }
      if (insertFlag > 0) {
        val backResult = HttpRequest.sendGet2(url, param)
        println(jc + "======table monitor_link_info_his insert成功")
        println(backResult + "======table monitor_link_info_his insert成功")
      } else {
        println(jc + "======table monitor_link_info_his insert失败")
      }
    } catch {
      case e: Exception => e.printStackTrace(); MailUtil.sendMailNew("头条推送", ExceptionMsgUtil.getStackTraceInfo(e))
    }
  }


  def macMd5sum(mac: String) = {
    /**
      * 　　* @Description: TODO mac取MD5sum
      * 　  * @param [mac]
      * 　　* @return java.lang.String
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/10/15 11:18
      * 　　*/
    if (StringUtils.isNotBlank(mac)) {
      val macStr = mac.replaceAll(":", "").toUpperCase
      Md5SumUtil.getMd5Sum(macStr)
    } else {
      ""
    }
  }

  def main(args: Array[String]): Unit = {
  }
}
