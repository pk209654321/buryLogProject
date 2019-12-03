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
        val user_id = splits(0)
        val phone_system = splits(7)
        //手机操作系统
        val mac = splits(14)
        //mac地址
        val imei = splits(15)
        //安卓设备信息
        val idfa = splits(18) //安卓设备信息
        (phone_system, mac, imei, idfa)
      } catch {
        case e: Exception => println(s"DataProcessingForTouTiao_error_log:logType=${one.logType},source=${one.source},line=${one.line}");("", "", "", "")
      }
    })
    //过滤掉空user_id和user_id为"0"的
    val filterData = phoneInfo.filter(line => {
      val str = line._1
      if (StringUtils.isNotBlank(str)) {
        true
      } else {
        false
      }
    })
    val disRdd = filterData.distinct()
    disRdd.foreachPartition(par => {
      // TODO: 加载数据库连接,加载近7天的监测数据
      //DBs.setupAll()
      DBs.setup('answer)
      DBs.setup('aurora)
      try {
        val jcData = NamedDB('answer).readOnly {
          implicit session =>
            SQL(s"SELECT os,mac,imei,idfa,callback_param FROM monitor_link_info WHERE DATE_ADD(CURDATE(),INTERVAL -7 DAY) <= DATE(create_time)").map(rs => {
              ((rs.get[String]("os"),
                rs.get[String]("mac"),
                rs.get[String]("imei"),
                rs.get[String]("idfa")),
                rs.get[String]("callback_param"))
            }).list().apply()
        }

        // TODO: 加载已经发送的历史数据
        val hisData = NamedDB('aurora).readOnly {
          implicit session =>
            SQL(s"SELECT os,mac,imei,idfa FROM monitor_link_info_his").map(rs => {
              (rs.get[String]("os"),
                rs.get[String]("mac"),
                rs.get[String]("imei"),
                rs.get[String]("idfa"))
            }).list().apply()
        }
        // TODO: 将检测数据去重并删除历史数据删除
        val newData = (jcData.toMap -- hisData).toList

        analysisPartition2(par, newData)

      } finally {
        DBs.closeAll()
      }
    })
  }


  def analysisPartition2(par: scala.Iterator[(String, String, String, String)]
                         , newData: scala.List[((String, String, String, String), String)]) {
    /**
      * 　　* @Description: TODO
      * 　　* @param [par, newData]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/10/13 20:54
      * 　　*/
    for (elem <- par) {
      val user_id = elem._1
      val phone_system = elem._1
      var mac = elem._2
      var imei = elem._3
      val idfa = elem._4
      mac = macMd5sum(mac)
      imei = Md5SumUtil.getMd5Sum(imei)
//      println("日志log===========" + elem)
      if (StringUtils.isNotBlank(phone_system)) {
        for (jc <- newData) {
          val jc_os = jc._1._1
          val jc_mac = jc._1._2
          val jc_imei = jc._1._3
          val jc_idfa = jc._1._4
          val jc_cp = jc._2
          if (jc_os.equals("0")) { //安卓
            if (StringUtils.isNotBlank(jc_imei)) {
              val bool_imei = jc_imei.equals(imei)
              if (bool_imei) {
                // TODO: 安卓类型回调头条接口
                insertAndPush2(jc)
              }
            } else {
              if (StringUtils.isNotBlank(jc_mac)) {
                //mac判断
                val bool_mac = jc_mac.equals(mac)
                if (bool_mac) {
                  // TODO: 安卓类型回调头条接口
                  insertAndPush2(jc)
                }
              }
            }
          } else if (jc_os.equals("1")) { //ios
            if (StringUtils.isNotBlank(jc_idfa)) {
              val bool_idfa = jc_idfa.equals(idfa)
              if (bool_idfa) {
                // TODO: ios类型回调头条接口
                insertAndPush2(jc)
              }
            } else {
              if (StringUtils.isNotBlank(jc_mac)) { //mac判断
                val bool_mac = jc_mac.equals(mac)
                if (bool_mac) {
                  // TODO: ios类型回调头条接口
                  insertAndPush2(jc)
                }
              }
            }
          } else {

          }
        }
      }
    }
  }


  def analysisPartition(par: scala.Iterator[(String, (String, String, String, String))]
                        , jcData: scala.List[(String, String, String, String, String)]
                        , his_mac: List[String]
                        , his_imei: List[String]
                        , his_idfa: List[String]) {
    /**
      * 　　* @Description: TODO
      * 　　* @param [par, jcData, his_mac, his_imei, his_idfa]
      * 　　* @return void
      * 　　* @throws
      * 　　* @author lenovo
      * 　　* @date 2019/10/13 20:54
      * 　　*/
    for (elem <- par) {
      val user_id = elem._1
      val phone_system = elem._2._1
      var mac = elem._2._2
      var imei = elem._2._3
      val idfa = elem._2._4
      mac = macMd5sum(mac)
      imei = Md5SumUtil.getMd5Sum(imei)
      if (StringUtils.isNotBlank(phone_system)) {
        for (jc <- jcData) {
          val jc_os = jc._1
          val jc_mac = jc._2
          val jc_imei = jc._3
          val jc_idfa = jc._4
          val jc_cp = jc._5
          if (jc_os.equals("0")) { //安卓
            if (StringUtils.isNotBlank(jc_imei)) {
              val bool_imei = jc_imei.equals(imei)
              if (bool_imei) {
                // TODO: 安卓类型回调头条接口
                val bl = his_imei.contains(jc_imei)
                if (!bl) {
                  insertAndPush(jc)
                }
              }
            } else {
              if (StringUtils.isNotBlank(jc_mac)) {
                //mac判断
                val bool_mac = jc_mac.equals(mac)
                if (bool_mac) {
                  // TODO: 安卓类型回调头条接口
                  val bl = his_mac.contains(jc_mac)
                  if (!bl) {
                    insertAndPush(jc)
                  }
                }
              }
            }
          } else if (jc_os.equals("1")) { //ios
            if (StringUtils.isNotBlank(jc_idfa)) {
              val bool_idfa = jc_idfa.equals(idfa)
              if (bool_idfa) {
                // TODO: ios类型回调头条接口
                val bl = his_idfa.contains(jc_idfa)
                if (!bl) {
                  insertAndPush(jc)
                }
              }
            } else {
              if (StringUtils.isNotBlank(jc_mac)) { //mac判断
                val bool_mac = jc_mac.equals(mac)
                if (bool_mac) {
                  // TODO: ios类型回调头条接口
                  val bl = his_mac.contains(jc_mac)
                  if (!bl) {
                    insertAndPush(jc)
                  }
                }
              }
            }
          } else {

          }
        }
      }
    }
  }

  def insertAndPush(jc: (String, String, String, String, String)) {
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
    val mac = jc._2
    val imei = jc._3
    val idfa = jc._4
    val callback_param = jc._5

    val os = jc._1
    os match {
      case "0" => muid = imei
      case "1" => muid = idfa
      case _ =>
    }
    param = s"muid=${muid}&os=${os}&oaid=&callback=${callback_param}&event_type=0"
    val insertFlag = NamedDB('offset).localTx {
      val result = println(HttpRequest.sendGet2(url, param))
      implicit session =>
        SQL("insert INTO monitor_link_info_his (os,mac,imei,idfa) VALUES (?,?,?,?)").bind(jc._1, jc._2, jc._3, jc._4).update().apply()
    }
    if (insertFlag > 0) {
      println(jc + "======table monitor_link_info_his insert成功")
    } else {
      println(jc + "======table monitor_link_info_his insert失败")
    }
  }

  def insertAndPush2(jc: ((String, String, String, String), String)) {
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
    param = s"muid=${muid}&os=${os}&callback=${callback_param}&event_type=0"
    val insertFlag = NamedDB('aurora).localTx {
      HttpRequest.sendGet2(url, param)
      implicit session =>
        SQL("insert INTO monitor_link_info_his (os,mac,imei,idfa) VALUES (?,?,?,?)").bind(os, mac, imei, idfa).update().apply()
    }
    if (insertFlag > 0) {
      println(jc + "======table monitor_link_info_his insert成功")
    } else {
      println(jc + "======table monitor_link_info_his insert失败")
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
    val macStr = mac.replaceAll(":", "")
    Md5SumUtil.getMd5Sum(macStr)
  }

  def main(args: Array[String]): Unit = {
    val map = List((1, 2), (1, 2)).toMap
    println(map)
    /*println(macMd5sum("cc:2d:83:0c:5f:e8"))
    println(macMd5sum("68:3E:34:F2:38:B6"))
    println(macMd5sum("5C:66:6C:2C:59:EB"))
    println(macMd5sum("B0:E5:ED:94:18:10"))
   println(Md5SumUtil.getMd5Sum("862787038081377"))
   println(Md5SumUtil.getMd5Sum("863328030912829"))
   println(Md5SumUtil.getMd5Sum("869208045211975"))
   println(Md5SumUtil.getMd5Sum("865762034021823"))*/
    //println(Md5SumUtil.getMd5Sum("869435038472757"))
    /*DBs.setupAll()
    insertAndPush(("0",
      "85c980f99cb17a027aeae009f26bbfa3",
      "29fa54f8ffe618d5f84c70b02bfe3fdf",
      "",
      "CKjo7K-ryPYCELjo7K-ryPYCGNnlrfEMIJqyl9XbASiLgKihs8D2AjAMOKGcAUIiMjAxOTEwMTQxOTIwMTUwMTAwMjYwNzcyMDcwNjAyQ0FEOUgBUAA="))*/

  }
}
