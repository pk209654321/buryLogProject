package sparkRealTime.touTiaoBackRealTime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import scalaUtil.{HttpRequest, MailUtil, Md5SumUtil}
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
    //清洗去掉不规则字符
    val allData = filterBlank.map(BuryCleanCommon.cleanCommonToListBuryLogin).filter(_.size() > 0)
    val rddOneObjcet: RDD[AnyRef] = allData.flatMap(_.toArray())
    //过滤空的line
    val allDataOneRdd = rddOneObjcet.map(_.asInstanceOf[BuryLogin]).filter(one => {
      val line = one.line
      StringUtils.isNotBlank(line)
    })
    //过滤出访问日志Data
    val filterVisit = allDataOneRdd.filter(_.logType == 1)
    val phoneInfo = filterVisit.map(one => {
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
        //val androidid = splits(22).trim

        (phone_system, mac, imei, idfa, guid)
      } catch {
        case e: Exception => println(s"DataProcessingForTouTiao_error_log:logType=${one.logType},source=${one.source},line=${one.line}"); ("", "", "", "", "")
      }
    })
    //过滤掉手机操作系统为空/""
    val filterData = phoneInfo.filter(line => {
      val os = line._1
      if (StringUtils.isNotBlank(os)) {
        true
      } else {
        false
      }
    })
    val disRdd = filterData.distinct()
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
            SQL(s"SELECT os,mac,imei,idfa,callback_param FROM monitor_link_info WHERE DATE_ADD(CURDATE(),INTERVAL -2 DAY) <= create_time").map(rs => {
              ((rs.get[String]("os"),
                rs.get[String]("mac"),
                rs.get[String]("imei"),
                rs.get[String]("idfa")
                //rs.get[String]("androidid")
              ),
                rs.get[String]("callback_param"))
            }).list().apply()
        }

        // TODO: 加载已经激活发送的历史数据
        val hisData = NamedDB('phpmanager).readOnly {
          implicit session =>
            SQL(s"SELECT os,mac,imei,idfa,callback_param FROM monitor_link_info_his").map(rs => {
              ((rs.get[String]("os"),
                rs.get[String]("mac"),
                rs.get[String]("imei"),
                rs.get[String]("idfa")
                //rs.get[String]("androidid")
              ),
                rs.get[String]("callback_param"))
            }).list().apply()
        }

        /* // TODO: 加载注册表
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
         val guidDiff = registerData.diff(registerDataHis)*/
        // TODO: 将检测数据去重并删除历史数据删除
        val newData = jcData.toSet.diff(hisData.toSet).toList
        analysisPartition(par, newData, hisData, null)
      } finally {
        DBs.close('db_sscf)
        DBs.close('phpmanager)
        DBs.close('db_user)
      }
    })
  }


  def analysisPartition(par: scala.Iterator[(String, String, String, String, String)]
                        , newData: scala.List[((String, String, String, String), String)]
                        , hisData: scala.List[((String, String, String, String), String)]
                        , guidDiff: List[String]) {
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
      var mac = elem._2
      var imei = elem._3
      val idfa = elem._4
      val guid = elem._5
      mac = macMd5sum(mac)
      imei = Md5SumUtil.getMd5Sum(imei)
      val parOne = (phone_system, mac, imei, idfa)
      //      println("日志log===========" + elem)
      // TODO: 1,激活上报
      for (jc <- newData) {
        val jcOne = jc._1
        val jc_os = jc._1._1
        val jc_mac = jc._1._2
        val jc_imei = jc._1._3
        val jc_idfa = jc._1._4
        if (jc_os.equals("0")) { //安卓
          if ((StringUtils.isNoneBlank(jc_imei) && jc_imei.equals(imei) && !jc_imei.equals("5284047f4ffb4e04824a2fd1d1f0cd62")) || (StringUtils.isNotBlank(jc_mac) && jc_mac.equals(mac))) {
            insertAndPush(jc, 0)
          }
        } else if (jc_os.equals("1")) { //ios
          if (StringUtils.isNotBlank(jc_idfa) && jc_idfa.equals(idfa) || (StringUtils.isNotBlank(jc_mac) && jc_mac.equals(mac))) {
            insertAndPush(jc, 0)
          }
        }
      }

      // TODO: 2,注册上报
      /*for (his <- hisData) {
        val his_os = his._1._1
        val his_mac = his._1._2
        val his_imei = his._1._3
        val his_idfa = his._1._4
        val hisOne = (his_os, his_mac, his_imei, his_idfa)
        if (parOne.equals(hisOne)) { //判断是否与日志中的设备参数一致
          if (guidDiff.contains(guid)) { //判断guid是否在过滤后的注册表中
            if (phone_system.equals("0")) { //安卓
              insertAndPushRegister(his, 1, guid)
            } else if (phone_system.equals("1")) { //ios
              insertAndPushRegister(his, 1, guid)
            }
          }
        }
      }*/
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
        HttpRequest.sendGet2(url, param)
        println(his + "======table monitor_link_info_his_register insert成功")
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
    val mac = jc._1._2
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
          SQL("insert INTO monitor_link_info_his (os,mac,imei,idfa,callback_param) VALUES (?,?,?,?,?)").bind(os, mac, imei, idfa, callback_param).update().apply()
      }
      if (insertFlag > 0) {
        HttpRequest.sendGet2(url, param)
        println(jc + "======table monitor_link_info_his insert成功")
      } else {
        println(jc + "======table monitor_link_info_his insert失败")
      }
    } catch {
      case e: Exception => e.printStackTrace()
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
    //0	9363d9e13b1b5302e7a689cac3a063f3	5284047f4ffb4e04824a2fd1d1f0cd62
    /*val os = "0222222"
    val mac = "9363d9e13b1b5302e7a689cac3a063f3"
    val imei = "5284047f4ffb4e04824a2fd1d1f0cd62"
    val idfa = ""
    val callback_param = "CIvwypnnivkCEI64ht3oivkCGJTKpKKdAiCFi-PSmgEoh-Cb0Ou89wIwDDi18QRCIjIwMjAwMjE4MDc0ODA2MDEwMDI3MDY5MDE0MTYyNzdEOUJIwbgCUAA="

    DBs.setup('phpmanager)
    var insertFlag = -1
    try {
      insertFlag = NamedDB('phpmanager).localTx {
        implicit session =>
          SQL("insert INTO monitor_link_info_his (os,mac,imei,idfa,callback_param) VALUES (?,?,?,?,?)").bind(os, mac, imei, idfa, callback_param).update().apply()
      }
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    println(insertFlag)*/


    /*val a = List(1, 2, 3, 1).toSet
    val b = List(5, 4, 3, 3, 2).toSet
    println(a.diff(b))*/
  }
}
